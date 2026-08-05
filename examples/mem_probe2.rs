// 复现 segment + translation 的窗口/batch 计算
use y2b_rs::subtitle::parse_vtt;
fn main() {
    let cues = parse_vtt(std::path::Path::new("/tmp/7MkC.vtt")).unwrap();
    println!("cues = {}", cues.len());
    // segment 窗口计算路径
    let budget = 200_000usize;
    let byte_budget = 96 * 1024;
    let mut window = 0usize;
    for start in (0..cues.len()).step_by(window.max(1)) {
        let mut total = 2_048usize;
        let mut bytes = 512usize;
        let mut end = None;
        for (index, cue) in cues.iter().enumerate().skip(start) {
            let item = cue.source.len() + 22;
            let item_bytes = serde_json::to_string(&serde_json::json!({"i":index-start,"start":cue.start,"end":cue.end,"text":cue.source})).map_or(usize::MAX, |v| v.len()+1);
            if total.saturating_add(item) > budget || bytes.saturating_add(item_bytes) > byte_budget
            {
                break;
            }
            total += item;
            bytes += item_bytes;
            end = Some(index);
        }
        let Some(e) = end else { break };
        if e + 1 >= cues.len() {
            window = cues.len() - start;
            break;
        }
        window = e - start + 1;
    }
    println!("segment windows total = {}", window);
    // translation batches
    let mut batches = 0;
    let mut start = 0;
    let mut total: usize = 2_048;
    for (i, cue) in cues.iter().enumerate() {
        let item = cue.source.len().saturating_mul(2) + 20;
        if i > start && (i - start >= 50 || total.saturating_add(item) > budget) {
            batches += 1;
            start = i;
            total = 2_048;
        }
        total += item;
    }
    batches += 1;
    println!("translation batches(50) = {batches}");
}
