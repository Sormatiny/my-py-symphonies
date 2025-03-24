import paramiko
import os

class SSHConnection(object):
    def __init__(self, host, username, password, port=22):
        self.sftp = None
        self.sftp_open = False
        self.transport = paramiko.Transport((host, port))
        self.transport.connect(username=username, password=password)

    def _openSFTPConnection(self):
        if not self.sftp_open:
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            self.sftp_open = True

    def put(self, local_path, remote_path=None):
        self._openSFTPConnection()
        self.sftp.put(local_path, remote_path)

    def close(self):
        if self.sftp_open:
            self.sftp.close()
            self.sftp_open = False
        self.transport.close()

if __name__ == "__main__":
    host = '127.0.0.1'
    username = 'usersftp'
    pw = 'usersftp'

    ssh = SSHConnection(host, username, pw)
    for root, dirs, files in os.walk('/home/atis/forsignal/2'):
        for file in files:
            if file.endswith('.gz'):
                origin = os.path.join(root, file)
                dst = os.path.join('/upload', file)
                ssh.put(origin, dst)
                os.remove(os.path.join(root, file))
    ssh.close()
