use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use csv::Writer;
use quick_xml::events::Event;
use quick_xml::Reader;
use std::time::Instant;

#[derive(ValueEnum, Clone, Copy, Debug)]
enum Dataset {
    #[value(name = "enwiki-abstract")]
    EnwikiAbstract,
    #[value(name = "enwiki-page")]
    EnwikiPage,
}

#[derive(Debug, serde::Serialize)]
#[serde(untagged)]
enum Record {
    Abstract {
        title: String,
        #[serde(rename = "abstract")]
        abstract_: String,
        url: String,
    },
    Page {
        title: String,
        text: String,
        comment: String,
        username: String,
        timestamp: String,
    },
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    /// 包含若干 *.xml 文件的目录
    #[arg(short = 'i', long, value_name = "DIR")]
    origin: PathBuf,

    /// 输出目录（不存在则创建）
    #[arg(short = 'o', long, value_name = "DIR", default_value = ".")]
    out_dir: PathBuf,

    /// 每个 CSV 文件的行数，默认 1
    #[arg(short = 'R', long, default_value_t = 1)]
    rows_per_file: usize,

    /// CSV 文件名前缀
    #[arg(short, long, default_value = "test.t")]
    csv_name: String,

    /// 数据集格式
    #[arg(short, long, value_enum, default_value_t = Dataset::EnwikiAbstract)]
    dataset: Dataset,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let start = Instant::now(); // 计时开始

    fs::create_dir_all(&cli.out_dir)?;
    if !cli.origin.is_dir() {
        bail!("--origin 必须是一个目录: {:?}", cli.origin);
    }

    let mut xml_files: Vec<PathBuf> = fs::read_dir(&cli.origin)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map_or(false, |ext| ext.eq_ignore_ascii_case("xml"))
        })
        .map(|e| e.path())
        .collect();

    xml_files.sort(); // PathBuf 实现了 Ord，可以排序
    let total_xml = xml_files.len();
    if total_xml == 0 {
        bail!("目录 {:?} 下没有找到 .xml 文件", cli.origin);
    }

    let mut wtr: Option<Writer<BufWriter<File>>> = None;
    let mut file_idx = 0usize;
    let mut total_rows = 0usize;

    for (idx, xml_path) in xml_files.iter().enumerate() {
        let file_start = Instant::now();
        let file = File::open(xml_path).with_context(|| format!("无法打开 {:?}", xml_path))?;
        let reader = BufReader::new(file);
        let mut xml = Reader::from_reader(reader);
        xml.config_mut().trim_text(true);
        process_one_xml(
            &mut xml,
            cli.dataset,
            &mut wtr,
            &mut file_idx,
            &mut total_rows,
            cli.rows_per_file,
            &cli.out_dir,
            &cli.csv_name,
        )?;
        eprintln!(
            "[{}/{}] 处理完成 {:?}，耗时 {:?}",
            idx + 1,
            total_xml,
            xml_path.file_name().unwrap(),
            file_start.elapsed()
        );
    }

    // flush 最后一个文件
    if let Some(w) = wtr.take() {
        w.into_inner()?.flush()?;
        eprintln!(
            "[CSV] 生成 {} 个文件，总计 {} 行，全部完成！总耗时 {:?}",
            file_idx,
            total_rows,
            start.elapsed()
        );
    }

    Ok(())
}

fn process_one_xml<R: std::io::BufRead>(
    xml: &mut Reader<R>,
    dataset: Dataset,
    wtr: &mut Option<Writer<BufWriter<File>>>,
    file_idx: &mut usize,
    total_rows: &mut usize,
    rows_per_file: usize,
    out_dir: &Path,
    csv_name: &str,
) -> Result<()> {
    match dataset {
        Dataset::EnwikiAbstract => parse_abstract(
            xml,
            wtr,
            file_idx,
            total_rows,
            rows_per_file,
            out_dir,
            csv_name,
        ),
        Dataset::EnwikiPage => parse_page(
            xml,
            wtr,
            file_idx,
            total_rows,
            rows_per_file,
            out_dir,
            csv_name,
        ),
    }
}

fn read_text<R: std::io::BufRead>(
    reader: &mut quick_xml::Reader<R>,
    end_tag: &[u8],
    buf: &mut Vec<u8>,
) -> Result<String, quick_xml::Error> {
    let mut txt = String::new();
    loop {
        match reader.read_event_into(buf)? {
            Event::Text(e) => txt.push_str(&e.unescape()?.into_owned()),
            Event::End(e) if e.name().as_ref() == end_tag => break,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    Ok(txt)
}

fn maybe_new_file(
    wtr: &mut Option<Writer<BufWriter<File>>>,
    file_idx: &mut usize,
    total_rows: usize,
    rows_per_file: usize,
    out_dir: &Path,
    csv_name: &str,
) -> Result<()> {
    if total_rows == 1 || (total_rows - 1) % rows_per_file == 0 {
        if let Some(w) = wtr.take() {
            w.into_inner()?.flush()?;
        }
        let path = out_dir.join(format!("{}.{}.csv", csv_name, *file_idx + 1));
        let file = BufWriter::new(File::create(path)?);
        *wtr = Some(Writer::from_writer(file));
        *file_idx += 1;
        eprintln!("[CSV] 新建文件 {}.{}.csv", csv_name, *file_idx);
    }
    Ok(())
}

// format is as follows:
// <feed>
// <doc>
// <title>Wikipedia: South Korea national under-18 baseball team</title>
// <url>https://en.wikipedia.org/wiki/South_Korea_national_under-18_baseball_team</url>
// <abstract>| Federation = Korea Baseball Association</abstract>
// <links>
// <sublink linktype="nav"><anchor>See also</anchor><link>https://en.wikipedia.org/wiki/South_Korea_national_under-18_baseball_team#See_also</link></sublink>
// <sublink linktype="nav"><anchor>References</anchor><link>https://en.wikipedia.org/wiki/South_Korea_national_under-18_baseball_team#References</link></sublink>
// </links>
// </doc>
// <doc>
// <title>Wikipedia: The Z Murders</title>
// <url>https://en.wikipedia.org/wiki/The_Z_Murders</url>
// <abstract>The Z "Murders" is a 1932 mystery crime novel by the British writer Joseph Jefferson Farjeon.Hubin p.</abstract>
// <links>
// <sublink linktype="nav"><anchor>Synopsis</anchor><link>https://en.wikipedia.org/wiki/The_Z_Murders#Synopsis</link></sublink>
// <sublink linktype="nav"><anchor>References</anchor><link>https://en.wikipedia.org/wiki/The_Z_Murders#References</link></sublink>
// </links>
// </doc>
// ...</feed>
fn parse_abstract<R: std::io::BufRead>(
    xml: &mut Reader<R>,
    wtr: &mut Option<Writer<BufWriter<File>>>,
    file_idx: &mut usize,
    total_rows: &mut usize,
    rows_per_file: usize,
    out_dir: &Path,
    csv_name: &str,
) -> Result<()> {
    let mut buf = Vec::new();
    let mut title = String::new();
    let mut abstract_ = String::new();
    let mut url = String::new();

    loop {
        match xml.read_event_into(&mut buf)? {
            Event::Start(ref e) if e.name().as_ref() == b"doc" => {
                title.clear();
                abstract_.clear();
                url.clear();
            }
            Event::End(ref e) if e.name().as_ref() == b"doc" => {
                *total_rows += 1;
                maybe_new_file(wtr, file_idx, *total_rows, rows_per_file, out_dir, csv_name)?;
                let rec = Record::Abstract {
                    title: title.trim().to_string(),
                    abstract_: abstract_.trim().to_string(),
                    url: url.trim().to_string(),
                };
                if let Some(w) = wtr {
                    w.serialize(rec)?;
                }
            }
            Event::Start(ref e) if e.name().as_ref() == b"title" => {
                title = read_text(xml, b"title", &mut buf)?;
            }
            Event::Start(ref e) if e.name().as_ref() == b"abstract" => {
                abstract_ = read_text(xml, b"abstract", &mut buf)?;
            }
            Event::Start(ref e) if e.name().as_ref() == b"url" && url.is_empty() => {
                url = read_text(xml, b"url", &mut buf)?;
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    Ok(())
}

// format is as follows:
// <mediawiki>
//   <page>
//     <title>Hop</title>
//     <ns>0</ns>
//     <id>41240</id>
//     <revision>
//       <id>1258369426</id>
//       <parentid>1237397543</parentid>
//       <timestamp>2024-11-19T09:05:40Z</timestamp>
//       <contributor>
//         <username>Jlwoodwa</username>
//         <id>45789152</id>
//       </contributor>
//       <comment>add [[Hop (mixtape)]]</comment>
//       <origin>1258369426</origin>
//       <model>wikitext</model>
//       <format>text/x-wiki</format>
//       <text bytes="2963" sha1="kg5qkez407n8dmt64u6fye28wkdhvhc" xml:space="preserve">{{Wiktionary|hop|hops|họp|hóp|hớp}}
// A '''hop''' is a type of [[Jumping|jump]].

// '''Hop''' or '''hops''' may also refer to:
// {{TOC right}}

// ==Arts and entertainment==
// </text>
//       <sha1>8vckxv82wu6g9axmb2y0hk2cjz6p8f3</sha1>
//     </revision>
//   </page>
// </mediawiki>
fn parse_page<R: std::io::BufRead>(
    xml: &mut Reader<R>,
    wtr: &mut Option<Writer<BufWriter<File>>>,
    file_idx: &mut usize,
    total_rows: &mut usize,
    rows_per_file: usize,
    out_dir: &Path,
    csv_name: &str,
) -> Result<()> {
    let mut buf = Vec::new();
    let mut title = String::new();
    let mut text = String::new();
    let mut comment = String::new();
    let mut username = String::new();
    let mut timestamp = String::new();

    loop {
        match xml.read_event_into(&mut buf)? {
            Event::Start(ref e) if e.name().as_ref() == b"page" => {
                title.clear();
                text.clear();
                comment.clear();
                username.clear();
                timestamp.clear();
            }
            Event::End(ref e) if e.name().as_ref() == b"page" => {
                *total_rows += 1;
                maybe_new_file(wtr, file_idx, *total_rows, rows_per_file, out_dir, csv_name)?;
                let rec = Record::Page {
                    title: title.trim().to_string(),
                    text: text.trim().to_string(),
                    comment: comment.trim().to_string(),
                    username: username.trim().to_string(),
                    timestamp: timestamp.trim().to_string(),
                };
                if let Some(w) = wtr {
                    w.serialize(rec)?;
                }
            }
            Event::Start(ref e) if e.name().as_ref() == b"title" => {
                title = read_text(xml, b"title", &mut buf)?;
            }
            Event::Start(ref e) if e.name().as_ref() == b"text" => {
                text = read_text(xml, b"text", &mut buf)?;
            }
            Event::Start(ref e) if e.name().as_ref() == b"comment" => {
                comment = read_text(xml, b"comment", &mut buf)?;
            }
            Event::Start(ref e) if e.name().as_ref() == b"username" => {
                username = read_text(xml, b"username", &mut buf)?;
            }
            Event::Start(ref e) if e.name().as_ref() == b"timestamp" => {
                timestamp = read_text(xml, b"timestamp", &mut buf)?;
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    Ok(())
}
