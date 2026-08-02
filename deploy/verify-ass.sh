#!/usr/bin/env bash
set -euo pipefail
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cat >"$work/test.ass" <<'ASS'
[Script Info]
ScriptType: v4.00+
PlayResX: 1280
PlayResY: 720
[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,Source Han Sans CN Medium,42,&H00FFFFFF,&H000000FF,&H00101010,&H80000000,0,0,0,0,100,100,0,0,1,3,0,2,40,40,50,1
[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,中文字幕测试\NEnglish subtitle test
ASS
ffmpeg -y -f lavfi -i color=c=0x202838:s=1280x720:d=5:r=30 -vf "ass=filename='$work/test.ass':fontsdir='/opt/y2b/fonts'" -threads 1 -c:v libx264 -pix_fmt yuv420p "$work/test.mp4"
ffprobe -v error -show_entries stream=codec_name,width,height,pix_fmt -of json "$work/test.mp4"
ffmpeg -y -ss 2 -i "$work/test.mp4" -frames:v 1 "$work/preview.png"
install -d /var/lib/y2b/checks
install -m 0644 "$work/test.mp4" /var/lib/y2b/checks/ass-smoke.mp4
install -m 0644 "$work/preview.png" /var/lib/y2b/checks/ass-smoke-preview.png
echo /var/lib/y2b/checks/ass-smoke-preview.png
