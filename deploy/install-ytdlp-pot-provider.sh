#!/usr/bin/env bash
set -euo pipefail

[[ ${EUID} -eq 0 ]] || { echo "must run as root" >&2; exit 1; }

# 固定 provider/plugin 同一版本并校验上游产物，避免 yt-dlp 在 YouTube 开始
# 强制 subtitles PO Token 后把“提取受限”静默误判成“没有自动字幕”。
provider_version=1.3.2
plugin_sha256=d51cf1c54e487137df749bd8778cceaa62304e6c5054c955b95f028f93ad6d57
source_sha256=3545ac7ffc0869498755cb3b4760a72fa2f176689d0890a6f5b898d163012ba2
release_base=https://github.com/Brainicism/bgutil-ytdlp-pot-provider

for command_name in curl node npm npx sha256sum tar yt-dlp; do
  command -v "$command_name" >/dev/null || {
    echo "missing command: $command_name" >&2
    exit 1
  }
done

node_major=$(node -p 'Number(process.versions.node.split(".")[0])')
if (( node_major < 22 )); then
  echo "bgutil provider requires Node.js >= 22; found $(node --version)" >&2
  exit 1
fi

tmp_dir=$(mktemp -d /tmp/y2b-pot-provider.XXXXXX)
trap 'rm -rf -- "$tmp_dir"' EXIT

plugin_archive="$tmp_dir/bgutil-ytdlp-pot-provider.zip"
source_archive="$tmp_dir/bgutil-ytdlp-pot-provider.tar.gz"
curl -fL "$release_base/releases/download/$provider_version/bgutil-ytdlp-pot-provider.zip" \
  -o "$plugin_archive"
curl -fL "$release_base/archive/refs/tags/$provider_version.tar.gz" \
  -o "$source_archive"
printf '%s  %s\n' "$plugin_sha256" "$plugin_archive" | sha256sum -c -
printf '%s  %s\n' "$source_sha256" "$source_archive" | sha256sum -c -

tar -xzf "$source_archive" -C "$tmp_dir"
source_dir="$tmp_dir/bgutil-ytdlp-pot-provider-$provider_version"
(
  cd "$source_dir/server"
  npm ci
  npx tsc
)
[[ -f "$source_dir/server/build/generate_once.js" ]] || {
  echo "provider build did not produce generate_once.js" >&2
  exit 1
}

provider_root=/opt/y2b/yt-dlp-pot-provider
release_root="$provider_root/releases"
release_dir="$release_root/$provider_version"
install -d -o root -g root -m 0755 "$release_root"
if [[ ! -d "$release_dir" ]]; then
  staged_release="$release_root/.$provider_version.$$.tmp"
  cp -a "$source_dir" "$staged_release"
  chown -R root:root "$staged_release"
  chmod -R go-w "$staged_release"
  mv -T "$staged_release" "$release_dir"
fi

if [[ -e "$provider_root/current" && ! -L "$provider_root/current" ]]; then
  echo "refusing to replace non-symlink: $provider_root/current" >&2
  exit 1
fi
current_link="$provider_root/.current.$$.tmp"
ln -s "$release_dir" "$current_link"
mv -Tf "$current_link" "$provider_root/current"

plugin_dir=/etc/yt-dlp/plugins
plugin_path="$plugin_dir/bgutil-ytdlp-pot-provider.zip"
install -d -o root -g root -m 0755 "$plugin_dir"
staged_plugin="$plugin_dir/.bgutil-ytdlp-pot-provider.$$.tmp.zip"
install -o root -g root -m 0644 "$plugin_archive" "$staged_plugin"
mv -f "$staged_plugin" "$plugin_path"

# y2b-watch.service 显式设置 HOME=/root；provider 的 script 模式会从此标准
# 路径自动发现 generate_once.js，不需要改 y2b 配置或重启正在运行的任务。
provider_home=/root/bgutil-ytdlp-pot-provider
if [[ -e "$provider_home" && ! -L "$provider_home" ]]; then
  echo "refusing to replace non-symlink: $provider_home" >&2
  exit 1
fi
home_link=/root/.bgutil-ytdlp-pot-provider.$$.tmp
ln -s "$provider_root/current" "$home_link"
mv -Tf "$home_link" "$provider_home"

installed_version=$(node "$provider_home/server/build/generate_once.js" --version)
[[ "$installed_version" == "$provider_version" ]] || {
  echo "provider version mismatch: expected=$provider_version actual=$installed_version" >&2
  exit 1
}

echo "bgutil provider=$installed_version"
echo "plugin=$plugin_path"
echo "mode=script-node (no listening port; y2b restart not required)"
