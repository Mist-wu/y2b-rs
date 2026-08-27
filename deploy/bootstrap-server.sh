#!/usr/bin/env bash
set -euo pipefail

[[ ${EUID} -eq 0 ]] || { echo "must run as root" >&2; exit 1; }
export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y ca-certificates curl jq xz-utils tar fontconfig gnupg python3

if ! swapon --show=NAME --noheadings | grep -qx /swapfile; then
  if [[ ! -f /swapfile ]]; then fallocate -l 2G /swapfile; fi
  chmod 600 /swapfile
  mkswap /swapfile >/dev/null
  swapon /swapfile
fi
grep -q '^/swapfile ' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
install -d /etc/sysctl.d
printf 'vm.swappiness=10\n' >/etc/sysctl.d/99-y2b-swap.conf
sysctl --system >/dev/null

if ! command -v node >/dev/null || [[ $(node -p 'Number(process.versions.node.split(".")[0])') -lt 24 ]]; then
  curl -fsSL https://deb.nodesource.com/setup_24.x | bash -
  apt-get install -y nodejs
fi
npm install -g @earendil-works/pi-coding-agent@0.83.0
pi_path=$(command -v pi)
if [[ "$pi_path" != /usr/local/bin/pi ]]; then ln -sfn "$pi_path" /usr/local/bin/pi; fi

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

yt_json=$(curl -fsSL https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest)
yt_url=$(jq -r '.assets[] | select(.name=="yt-dlp_linux") | .browser_download_url' <<<"$yt_json")
yt_digest=$(jq -r '.assets[] | select(.name=="yt-dlp_linux") | .digest' <<<"$yt_json" | cut -d: -f2)
curl -fL "$yt_url" -o "$tmp_dir/yt-dlp"
echo "$yt_digest  $tmp_dir/yt-dlp" | sha256sum -c -
install -m 0755 "$tmp_dir/yt-dlp" /usr/local/bin/yt-dlp

pot_installer=$(cd "$(dirname "$0")" && pwd)/install-ytdlp-pot-provider.sh
[[ -f "$pot_installer" ]] || {
  echo "missing companion installer: $pot_installer" >&2
  exit 1
}
bash "$pot_installer"

ff_json=$(curl -fsSL https://api.github.com/repos/BtbN/FFmpeg-Builds/releases/latest)
ff_name=ffmpeg-n8.1-latest-linux64-gpl-8.1.tar.xz
ff_url=$(jq -r --arg n "$ff_name" '.assets[] | select(.name==$n) | .browser_download_url' <<<"$ff_json")
ff_digest=$(jq -r --arg n "$ff_name" '.assets[] | select(.name==$n) | .digest' <<<"$ff_json" | cut -d: -f2)
curl -fL "$ff_url" -o "$tmp_dir/ffmpeg.tar.xz"
echo "$ff_digest  $tmp_dir/ffmpeg.tar.xz" | sha256sum -c -
tar -xJf "$tmp_dir/ffmpeg.tar.xz" -C "$tmp_dir"
ff_root=$(find "$tmp_dir" -maxdepth 1 -type d -name 'ffmpeg-*' | head -1)
install -m 0755 "$ff_root/bin/ffmpeg" /usr/local/bin/ffmpeg
install -m 0755 "$ff_root/bin/ffprobe" /usr/local/bin/ffprobe

bili_json=$(curl -fsSL https://api.github.com/repos/biliup/biliup/releases/latest)
bili_url=$(jq -r '.assets[] | select(.name|test("x86_64-linux-musl\\.tar\\.xz$")) | .browser_download_url' <<<"$bili_json")
bili_digest=$(jq -r '.assets[] | select(.name|test("x86_64-linux-musl\\.tar\\.xz$")) | .digest' <<<"$bili_json" | cut -d: -f2)
curl -fL "$bili_url" -o "$tmp_dir/biliup.tar.xz"
echo "$bili_digest  $tmp_dir/biliup.tar.xz" | sha256sum -c -
tar -xJf "$tmp_dir/biliup.tar.xz" -C "$tmp_dir"
bili_bin=$(find "$tmp_dir" -type f -name biliup | head -1)
install -m 0755 "$bili_bin" /usr/local/bin/biliup

install -d /opt/y2b/pi /var/lib/y2b/{downloads,backups} /etc/y2b
install -d -o root -g root -m 0700 /var/lib/y2b/pi-agent
if [[ ! -f /etc/y2b/y2b.env ]]; then
  install -o root -g root -m 0600 /dev/null /etc/y2b/y2b.env
fi

echo "node=$(node --version)"
echo "pi=$(pi --version)"
echo "yt-dlp=$(yt-dlp --version)"
ffmpeg -version | head -1
biliup --version
