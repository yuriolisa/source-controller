/*
Copyright 2022 The Flux authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
This was inspired and contains part of:
https://github.com/libgit2/git2go/blob/eae00773cce87d5282a8ac7c10b5c1961ee6f9cb/ssh.go

The MIT License

Copyright (c) 2013 The git2go contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package managed

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"

	git2go "github.com/libgit2/git2go/v33"
)

// registerManagedSSH registers a Go-native implementation of
// SSH transport that doesn't rely on any lower-level libraries
// such as libssh2.
//
// The underlying SSH connections are kept open and are reused
// across several SSH sessions. This is due to upstream issues in
// which concurrent/parallel SSH connections may lead to instability.
//
// Connections are created on first attempt to use a given remote. The
// connection is removed from the cache on the first failed session related
// operation.
//
// https://github.com/golang/go/issues/51926
// https://github.com/golang/go/issues/27140
func registerManagedSSH() error {
	for _, protocol := range []string{"ssh", "ssh+git", "git+ssh"} {
		_, err := git2go.NewRegisteredSmartTransport(protocol, false, sshSmartSubtransportFactory)
		if err != nil {
			return fmt.Errorf("failed to register transport for %q: %v", protocol, err)
		}
	}
	return nil
}

func sshSmartSubtransportFactory(remote *git2go.Remote, transport *git2go.Transport) (git2go.SmartSubtransport, error) {
	return &sshSmartSubtransport{
		transport: transport,
	}, nil
}

type sshSmartSubtransport struct {
	transport *git2go.Transport

	lastAction    git2go.SmartServiceAction
	client        *ssh.Client
	session       *ssh.Session
	stdin         io.WriteCloser
	stdout        io.Reader
	currentStream *sshSmartSubtransportStream
}

// aMux is the read-write mutex to control access to sshClients.
var aMux sync.RWMutex

// sshClients stores active ssh clients/connections to be reused.
//
// Once opened, connections will be kept cached until an error occurs
// during SSH commands, by which point it will be discarded, leading to
// a follow-up cache miss.
//
// The key must be based on cacheKey, refer to that function's comments.
var sshClients map[string]*ssh.Client = make(map[string]*ssh.Client)

func (t *sshSmartSubtransport) Action(urlString string, action git2go.SmartServiceAction) (git2go.SmartSubtransportStream, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	u, err := url.Parse(urlString)
	if err != nil {
		return nil, err
	}

	// Escape \ and '.
	uPath := strings.Replace(u.Path, `\`, `\\`, -1)
	uPath = strings.Replace(uPath, `'`, `\'`, -1)

	// TODO: Add percentage decode similar to libgit2.
	// Refer: https://github.com/libgit2/libgit2/blob/358a60e1b46000ea99ef10b4dd709e92f75ff74b/src/str.c#L455-L481

	var cmd string
	switch action {
	case git2go.SmartServiceActionUploadpackLs, git2go.SmartServiceActionUploadpack:
		if t.currentStream != nil {
			if t.lastAction == git2go.SmartServiceActionUploadpackLs {
				return t.currentStream, nil
			}
			t.Close()
		}
		cmd = fmt.Sprintf("git-upload-pack '%s'", uPath)

	case git2go.SmartServiceActionReceivepackLs, git2go.SmartServiceActionReceivepack:
		if t.currentStream != nil {
			if t.lastAction == git2go.SmartServiceActionReceivepackLs {
				return t.currentStream, nil
			}
			t.Close()
		}
		cmd = fmt.Sprintf("git-receive-pack '%s'", uPath)

	default:
		return nil, fmt.Errorf("unexpected action: %v", action)
	}

	cred, err := t.transport.SmartCredentials("", git2go.CredentialTypeSSHKey|git2go.CredentialTypeSSHMemory)
	if err != nil {
		return nil, err
	}
	defer cred.Free()

	var addr string
	if u.Port() != "" {
		addr = fmt.Sprintf("%s:%s", u.Hostname(), u.Port())
	} else {
		addr = fmt.Sprintf("%s:22", u.Hostname())
	}

	ckey, sshConfig, err := cacheKeyAndConfig(addr, cred)
	if err != nil {
		return nil, err
	}
	sshConfig.HostKeyCallback = func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		marshaledKey := key.Marshal()
		cert := &git2go.Certificate{
			Kind: git2go.CertificateHostkey,
			Hostkey: git2go.HostkeyCertificate{
				Kind:         git2go.HostkeySHA1 | git2go.HostkeyMD5 | git2go.HostkeySHA256 | git2go.HostkeyRaw,
				HashMD5:      md5.Sum(marshaledKey),
				HashSHA1:     sha1.Sum(marshaledKey),
				HashSHA256:   sha256.Sum256(marshaledKey),
				Hostkey:      marshaledKey,
				SSHPublicKey: key,
			},
		}

		return t.transport.SmartCertificateCheck(cert, true, hostname)
	}

	aMux.RLock()
	if c, ok := sshClients[ckey]; ok {
		traceLog.Info("[ssh]: cache hit", "remoteAddress", addr)
		t.client = c
	}
	aMux.RUnlock()

	if t.client == nil {
		traceLog.Info("[ssh]: cache miss", "remoteAddress", addr)

		aMux.Lock()
		defer aMux.Unlock()

		// In some scenarios the ssh handshake can hang indefinitely at
		// golang.org/x/crypto/ssh.(*handshakeTransport).kexLoop.
		//
		// xref: https://github.com/golang/go/issues/51926
		done := make(chan error, 1)
		go func() {
			t.client, err = ssh.Dial("tcp", addr, sshConfig)
			done <- err
		}()

		dialTimeout := sshConfig.Timeout + (30 * time.Second)

		select {
		case doneErr := <-done:
			if doneErr != nil {
				err = fmt.Errorf("ssh.Dial: %w", doneErr)
			}
		case <-time.After(dialTimeout):
			err = fmt.Errorf("timed out waiting for ssh.Dial after %s", dialTimeout)
		}

		if err != nil {
			return nil, err
		}

		sshClients[ckey] = t.client
	}

	traceLog.Info("[ssh]: creating new ssh session")
	if t.session, err = t.client.NewSession(); err != nil {
		discardCachedSshClient(ckey)
		return nil, err
	}

	if t.stdin, err = t.session.StdinPipe(); err != nil {
		discardCachedSshClient(ckey)
		return nil, err
	}

	if t.stdout, err = t.session.StdoutPipe(); err != nil {
		discardCachedSshClient(ckey)
		return nil, err
	}

	traceLog.Info("[ssh]: run on remote", "cmd", cmd)
	if err := t.session.Start(cmd); err != nil {
		discardCachedSshClient(ckey)
		return nil, err
	}

	t.lastAction = action
	t.currentStream = &sshSmartSubtransportStream{
		owner: t,
	}

	return t.currentStream, nil
}

func (t *sshSmartSubtransport) Close() error {
	var returnErr error

	traceLog.Info("[ssh]: sshSmartSubtransport.Close()")
	t.currentStream = nil
	if t.client != nil {
		if err := t.stdin.Close(); err != nil {
			returnErr = fmt.Errorf("cannot close stdin: %w", err)
		}
		t.client = nil
	}
	if t.session != nil {
		traceLog.Info("[ssh]: skipping session.wait")
		traceLog.Info("[ssh]: session.Close()")
		if err := t.session.Close(); err != nil {
			returnErr = fmt.Errorf("cannot close session: %w", err)
		}
	}

	return returnErr
}

func (t *sshSmartSubtransport) Free() {
	traceLog.Info("[ssh]: sshSmartSubtransport.Free()")
}

type sshSmartSubtransportStream struct {
	owner *sshSmartSubtransport
}

func (stream *sshSmartSubtransportStream) Read(buf []byte) (int, error) {
	return stream.owner.stdout.Read(buf)
}

func (stream *sshSmartSubtransportStream) Write(buf []byte) (int, error) {
	return stream.owner.stdin.Write(buf)
}

func (stream *sshSmartSubtransportStream) Free() {
	traceLog.Info("[ssh]: sshSmartSubtransportStream.Free()")
}

func cacheKeyAndConfig(remoteAddress string, cred *git2go.Credential) (string, *ssh.ClientConfig, error) {
	username, _, privatekey, passphrase, err := cred.GetSSHKey()
	if err != nil {
		return "", nil, err
	}

	var pemBytes []byte
	if cred.Type() == git2go.CredentialTypeSSHMemory {
		pemBytes = []byte(privatekey)
	} else {
		return "", nil, fmt.Errorf("file based SSH credential is not supported")
	}

	// must include the passphrase, otherwise a caller that knows the private key, but
	// not its passphrase would be able to bypass auth.
	ck := cacheKey(remoteAddress, username, passphrase, pemBytes)

	var key ssh.Signer
	if passphrase != "" {
		key, err = ssh.ParsePrivateKeyWithPassphrase(pemBytes, []byte(passphrase))
	} else {
		key, err = ssh.ParsePrivateKey(pemBytes)
	}

	if err != nil {
		return "", nil, err
	}

	cfg := &ssh.ClientConfig{
		User:    username,
		Auth:    []ssh.AuthMethod{ssh.PublicKeys(key)},
		Timeout: sshConnectionTimeOut,
	}

	return ck, cfg, nil
}

// cacheKey generates a cache key that is multi-tenancy safe.
//
// Stablishing multiple and concurrent ssh connections leads to stability
// issues documented above. However, the caching/sharing of already stablished
// connections could represent a vector for users to bypass the ssh authentication
// mechanism.
//
// cacheKey tries to ensure that connections are only shared by users that
// have the exact same remoteAddress and credentials.
func cacheKey(remoteAddress, userName, passphrase string, pubKey []byte) string {
	h := sha256.New()

	v := fmt.Sprintf("%s-%s-%s-%v", remoteAddress, userName, passphrase, pubKey)

	h.Write([]byte(v))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// discardCachedSshClient discards the cached ssh client, forcing the next git operation
// to create a new one via ssh.Dial.
func discardCachedSshClient(key string) {
	aMux.Lock()
	defer aMux.Unlock()

	if _, found := sshClients[key]; found {
		traceLog.Info("[ssh]: discard cached ssh client")
		delete(sshClients, key)
	}
}
