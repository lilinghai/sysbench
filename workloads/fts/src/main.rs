use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use csv::Writer;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field, Row};
use quick_xml::events::Event;
use quick_xml::Reader;
use std::time::Instant;

#[derive(ValueEnum, Clone, Copy, Debug)]
enum Dataset {
    #[value(name = "enwiki-abstract")]
    EnwikiAbstract,
    #[value(name = "enwiki-page")]
    EnwikiPage,
    #[value(name = "amazon-review")]
    AmazonReview,
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
    AmazonReview {
        review_date: u16,
        marketplace: String,
        customer_id: u64,
        review_id: String,
        product_id: String,
        product_parent: u64,
        product_title: String,
        product_category: String,
        star_rating: u8,
        helpful_votes: u32,
        total_votes: u32,
        vine: bool,
        verified_purchase: bool,
        review_headline: String,
        review_body: String,
    },
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    /// 包含若干 *.xml 或 parquet 文件的目录
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
    let start = Instant::now();

    fs::create_dir_all(&cli.out_dir)?;
    if !cli.origin.is_dir() {
        bail!("--origin 必须是一个目录: {:?}", cli.origin);
    }

    let expect_ext = match cli.dataset {
        Dataset::EnwikiAbstract | Dataset::EnwikiPage => "xml",
        Dataset::AmazonReview => "parquet",
    };

    let mut files: Vec<PathBuf> = fs::read_dir(&cli.origin)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map_or(false, |ext| ext.eq_ignore_ascii_case(expect_ext))
        })
        .map(|e| e.path())
        .collect();

    files.sort();
    if files.is_empty() {
        bail!("目录 {:?} 下没有找到 .{} 文件", cli.origin, expect_ext);
    }

    let mut wtr: Option<Writer<BufWriter<File>>> = None;
    let mut file_idx = 0usize;
    let mut total_rows = 0usize;

    for (idx, file_path) in files.iter().enumerate() {
        let file_start = Instant::now();
        process_one_file(
            &file_path,
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
            files.len(),
            file_path.file_name().unwrap(),
            file_start.elapsed()
        );
    }

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

fn process_one_file(
    file_path: &Path,
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
            file_path,
            wtr,
            file_idx,
            total_rows,
            rows_per_file,
            out_dir,
            csv_name,
        ),
        Dataset::EnwikiPage => parse_page(
            file_path,
            wtr,
            file_idx,
            total_rows,
            rows_per_file,
            out_dir,
            csv_name,
        ),
        Dataset::AmazonReview => parse_amazon_review(
            file_path,
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
) -> Result<bool> {
    if total_rows == 1
        || (total_rows - 1) % rows_per_file == 0
        // for batch write csv
        || total_rows >= rows_per_file * (*file_idx+1)
    {
        if let Some(w) = wtr.take() {
            w.into_inner()?.flush()?;
        }
        let path = out_dir.join(format!("{}.{}.csv", csv_name, *file_idx + 1));
        let file = BufWriter::new(File::create(path)?);
        *wtr = Some(Writer::from_writer(file));
        *file_idx += 1;
        eprintln!("[CSV] 新建文件 {}.{}.csv", csv_name, *file_idx);
        return Ok(true);
    }
    Ok(false)
}

// Schema:
//   0: review_date (INT32)
//   1: marketplace (BYTE_ARRAY)
//   2: customer_id (INT64)
//   3: review_id (BYTE_ARRAY)
//   4: product_id (BYTE_ARRAY)
//   5: product_parent (INT64)
//   6: product_title (BYTE_ARRAY)
//   7: product_category (BYTE_ARRAY)
//   8: star_rating (INT32)
//   9: helpful_votes (INT32)
//   10: total_votes (INT32)
//   11: vine (BOOLEAN)
//   12: verified_purchase (BOOLEAN)
//   13: review_headline (BYTE_ARRAY)
//   14: review_body (BYTE_ARRAY)
fn parse_amazon_review(
    file_path: &Path,
    wtr: &mut Option<csv::Writer<BufWriter<File>>>,
    file_idx: &mut usize,
    total_rows: &mut usize,
    rows_per_file: usize,
    out_dir: &Path,
    csv_name: &str,
) -> Result<()> {
    let file = File::open(file_path)?;
    let reader = SerializedFileReader::new(file)?;
    let row_iter = reader.get_row_iter(None)?;

    for row_res in row_iter {
        let row: Row = row_res?;
        let col = |i: usize| -> &Field { row.get_column_iter().nth(i).unwrap().1 };

        let rec = Record::AmazonReview {
            review_date: match col(0) {
                Field::UShort(v) => *v,
                _ => 0,
            },
            marketplace: match col(1) {
                Field::Bytes(b) => String::from_utf8_lossy(b.data()).into_owned(),
                _ => "".to_string(),
            },
            customer_id: match col(2) {
                Field::ULong(v) => *v,
                _ => 0,
            },
            review_id: match col(3) {
                Field::Bytes(b) => String::from_utf8_lossy(b.data()).into_owned(),
                _ => "".to_string(),
            },
            product_id: match col(4) {
                Field::Bytes(b) => String::from_utf8_lossy(b.data()).into_owned(),
                _ => "".to_string(),
            },
            product_parent: match col(5) {
                Field::ULong(v) => *v,
                _ => 0,
            },
            product_title: match col(6) {
                Field::Bytes(b) => String::from_utf8_lossy(b.data()).into_owned(),
                _ => "".to_string(),
            },
            product_category: match col(7) {
                Field::Bytes(b) => String::from_utf8_lossy(b.data()).into_owned(),
                _ => "".to_string(),
            },
            star_rating: match col(8) {
                Field::UByte(v) => *v,
                _ => 0,
            },
            helpful_votes: match col(9) {
                Field::UInt(v) => *v,
                _ => 0,
            },
            total_votes: match col(10) {
                Field::UInt(v) => *v,
                _ => 0,
            },
            vine: match col(11) {
                Field::Bool(v) => *v,
                _ => false,
            },
            verified_purchase: match col(12) {
                Field::Bool(v) => *v,
                _ => false,
            },
            review_headline: match col(13) {
                Field::Bytes(b) => String::from_utf8_lossy(b.data()).into_owned(),
                _ => "".to_string(),
            },
            review_body: match col(14) {
                Field::Bytes(b) => String::from_utf8_lossy(b.data()).into_owned(),
                _ => "".to_string(),
            },
        };

        *total_rows += 1;
        maybe_new_file(wtr, file_idx, *total_rows, rows_per_file, out_dir, csv_name)?;
        if let Some(w) = wtr {
            w.serialize(rec)?;
        }
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
fn parse_abstract(
    file_path: &Path,
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
    let file = File::open(file_path).with_context(|| format!("无法打开 {:?}", file_path))?;
    let reader = BufReader::new(file);
    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);

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
                title = read_text(&mut xml, b"title", &mut buf)?;
            }
            Event::Start(ref e) if e.name().as_ref() == b"abstract" => {
                abstract_ = read_text(&mut xml, b"abstract", &mut buf)?;
            }
            Event::Start(ref e) if e.name().as_ref() == b"url" && url.is_empty() => {
                url = read_text(&mut xml, b"url", &mut buf)?;
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
fn parse_page(
    file_path: &Path,
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

    let file = File::open(file_path).with_context(|| format!("无法打开 {:?}", file_path))?;
    let reader = BufReader::new(file);
    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);

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
                title = read_text(&mut xml, b"title", &mut buf)?;
            }
            Event::Start(ref e) if e.name().as_ref() == b"text" => {
                text = read_text(&mut xml, b"text", &mut buf)?;
            }
            Event::Start(ref e) if e.name().as_ref() == b"comment" => {
                comment = read_text(&mut xml, b"comment", &mut buf)?;
            }
            Event::Start(ref e) if e.name().as_ref() == b"username" => {
                username = read_text(&mut xml, b"username", &mut buf)?;
            }
            Event::Start(ref e) if e.name().as_ref() == b"timestamp" => {
                timestamp = read_text(&mut xml, b"timestamp", &mut buf)?;
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    Ok(())
}
