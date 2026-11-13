#!/bin/sh
# One image, two roles. Each role deletes the private key it must not hold,
# so the far host can only reach the dialer through the restricted tunnel key.
set -eu

home=/home/ferry
ssh-keygen -A >/dev/null

as_ferry() {
    su ferry -s /bin/sh -c "$1"
}

case "${FERRY_ROLE:?}" in
dialer)
    rm -f "$home/.ssh/tunnel_key"
    printf 'restrict,port-forwarding,permitlisten="localhost:2222",command="/usr/bin/false" %s\n' \
        "$(cat "$home/.ssh/tunnel_key.pub")" >"$home/.ssh/authorized_keys"
    cat >"$home/.ssh/config" <<EOF
Host far
  HostName localhost
  Port 2222
  IdentityFile ~/.ssh/dialer_key
  IdentitiesOnly yes
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  LogLevel ERROR
EOF
    cat >"$home/.config/ferry/config.toml" <<EOF
handlers = "~/.config/ferry/handlers"
connect = "far"
remote_ferry = "/opt/ferry/bin/ferry"
EOF
    chown -R ferry:ferry "$home"
    # A dead tunnel session must let go of port 2222, or the far host cannot reopen it.
    /usr/sbin/sshd -o PasswordAuthentication=no -o ClientAliveInterval=2 -o ClientAliveCountMax=2
    as_ferry 'python3 /opt/test/service.py >>"$HOME/service.log" 2>&1 &'
    as_ferry '/opt/ferry/bin/ferry daemon >>"$HOME/ferry.log" 2>&1 &'
    ;;
far)
    rm -f "$home/.ssh/dialer_key"
    cp "$home/.ssh/dialer_key.pub" "$home/.ssh/authorized_keys"
    cat >"$home/.ssh/config" <<EOF
Host dialer
  HostName ${DIALER_HOST:?}
  IdentityFile ~/.ssh/tunnel_key
  IdentitiesOnly yes
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  LogLevel ERROR
  ServerAliveInterval 2
  ServerAliveCountMax 2
  ExitOnForwardFailure yes
EOF
    cat >"$home/.config/ferry/config.toml" <<EOF
[[listen]]
port = 7811
handler = "service-proxy"
EOF
    chown -R ferry:ferry "$home"
    /usr/sbin/sshd -o PasswordAuthentication=no -o ListenAddress=127.0.0.1
    as_ferry 'AUTOSSH_GATETIME=0 AUTOSSH_POLL=5 autossh -M 0 -f -N -R localhost:2222:localhost:22 dialer'
    ;;
*)
    echo "unknown FERRY_ROLE ${FERRY_ROLE}" >&2
    exit 2
    ;;
esac

echo ready
exec sleep infinity
