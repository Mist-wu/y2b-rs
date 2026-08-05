// 用真实 VTT 复现 parse_vtt + dedup 链路，观察内存
fn main() {
    let path = std::path::Path::new("/tmp/7MkC.vtt");
    let cues = y2b_rs::subtitle::parse_vtt(path).unwrap();
    println!("cues = {}", cues.len());
    let mut total = 0usize;
    for c in &cues {
        total += c.source.len() + c.translation.as_deref().map(|t| t.len()).unwrap_or(0);
    }
    println!("text bytes = {total}");
}
