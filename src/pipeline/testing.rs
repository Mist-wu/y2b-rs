//! 各流水线子模块共用的测试夹具。

use crate::model::VideoMetadata;
use crate::subtitle::Cue;

pub fn cue(index: usize, text: &str) -> Cue {
    Cue {
        start: index as f64 * 2.0,
        end: index as f64 * 2.0 + 1.8,
        source: text.into(),
        translation: None,
    }
}

pub fn metadata() -> VideoMetadata {
    VideoMetadata {
        id: "video".into(),
        url: "https://youtube.com/watch?v=video".into(),
        title: "Best Ranked Match 2026".into(),
        description: Some("A close Brawl Stars ranked match.".into()),
        uploader: Some("Player One".into()),
        upload_date: Some("20260803".into()),
        channel: Some("Player One".into()),
        channel_id: Some("UC-test".into()),
        timestamp: Some(1_775_347_200),
        duration: Some(120.0),
        width: Some(1920),
        height: Some(1080),
        fps: Some(60.0),
        thumbnail_url: Some("https://i.ytimg.com/vi/video/maxresdefault.jpg".into()),
        webpage_url: Some("https://www.youtube.com/watch?v=video".into()),
        live_status: Some("not_live".into()),
    }
}
