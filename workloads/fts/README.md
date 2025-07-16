
# Use cases
## Enwiki abstract

Enwiki-abstract [details here](https://en.wikipedia.org/wiki/Wikipedia:Database_download), from English-language Wikipedia:Database page abstracts. This use case generates 3 TEXT fields per document, and focusses on full text queries performance.

### Example document

```sql
mysql> select * from wiki_abstract limit 1 \G
*************************** 1. row ***************************
   title: Wikipedia: Anarchism
abstract: Anarchism is a political philosophy and movement that is sceptical of authority and rejects all involuntary, coercive forms of hierarchy. Anarchism calls for the abolition of the state, which it holds to be unnecessary, undesirable, and harmful.
     url: https://en.wikipedia.org/wiki/Anarchism
      id: 1
```

### Document statistics

The size of csv file is about 170MB for 1 million rows 
```sql
select avg(char_length(title)),avg(char_length(url)),avg(char_length(abstract)) from wiki_abstract;
+-------------------------+-----------------------+----------------------------+
| avg(char_length(title)) | avg(char_length(url)) | avg(char_length(abstract)) |
+-------------------------+-----------------------+----------------------------+
|                 30.7858 |               50.5256 |                    92.2366 |
+-------------------------+-----------------------+----------------------------+
select min(char_length(`abstract`)), max(char_length(`abstract`)) from wiki_abstract;
+------------------------------+------------------------------+
| min(char_length(`abstract`)) | max(char_length(`abstract`)) |
+------------------------------+------------------------------+
|                            0 |                         1024 |
+------------------------------+------------------------------+
```
The dataset addresses(each have 6.5 million rows) that can be found online are:  
https://dumps.wikimedia.your.org/enwiki/20220520/enwiki-20220520-abstract.xml.gz  
https://dumps.wikimedia.your.org/enwiki/20220601/enwiki-20220601-abstract.xml.gz  
https://dumps.wikimedia.your.org/enwiki/20220620/enwiki-20220620-abstract.xml.gz  
https://dumps.wikimedia.your.org/enwiki/20220701/enwiki-20220701-abstract.xml.gz  
https://dumps.wikimedia.your.org/enwiki/20220720/enwiki-20220720-abstract.xml.gz  
https://dumps.wikimedia.your.org/enwiki/20220720/enwiki-20220720-abstract.xml.gz  
https://dumps.wikimedia.your.org/enwiki/20220801/enwiki-20220801-abstract.xml.gz  
https://dumps.wikimedia.your.org/enwiki/20220820/enwiki-20220820-abstract.xml.gz  
https://dumps.wikimedia.your.org/enwiki/latest/enwiki-latest-abstract.xml.gz  

### Generate data for TiDB

1. create table `fts`.`wiki_abstract`
```sql
CREATE TABLE `wiki_abstract` (
  `title` varchar(256) DEFAULT NULL,
  `abstract` text DEFAULT NULL,
  `url` varchar(256) DEFAULT NULL,
  `id` bigint NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
)
```
2. download data and decompress the files to `xml` dir
3. generate csv files
 ```bash
 ./fts -i xml -R 1000000 -c fts.wiki_abstract --dataset enwiki-abstract
 ```
 4. use tidb-lightning import the csv files

## Enwiki pages

Enwiki-pages [details here](https://en.wikipedia.org/wiki/Wikipedia:Database_download), from English-language Wikipedia:Database last page revisions, containing processed metadata extracted from the full Wikipedia XML dumppage abstracts. This use case generates 5 TEXT fields per document, and focuses on full text queries performance.

### Example document

```sql
select * from wiki_page limit 1 \G
*************************** 1. row ***************************
    title: AccessibleComputing
     text: #REDIRECT [[Computer accessibility]]

{{rcat shell|
{{R from move}}
{{R from CamelCase}}
{{R unprintworthy}}
}}
  comment: Restored revision 1002250816 by [[Special:Contributions/Elli|Elli]] ([[User talk:Elli|talk]]): Unexplained redirect breaking
 username: Asparagusus
timestamp: 2024-04-15 14:38:04
       id: 1
```

### Document statistics

The size of csv file is about 150MB ~ 200MB for 5,000 rows

```sql
select avg(char_length(title)),avg(char_length(`text`)),avg(char_length(`comment`)),avg(char_length(username)),avg(char_length(`timestamp`)) from wiki_page;
+-------------------------+--------------------------+-----------------------------+----------------------------+-------------------------------+
| avg(char_length(title)) | avg(char_length(`text`)) | avg(char_length(`comment`)) | avg(char_length(username)) | avg(char_length(`timestamp`)) |
+-------------------------+--------------------------+-----------------------------+----------------------------+-------------------------------+
|                 15.0592 |               26100.4464 |                     66.6281 |                     8.8153 |                       19.0000 |
+-------------------------+--------------------------+-----------------------------+----------------------------+-------------------------------+
select min(char_length(`text`)), max(char_length(`text`)) from wiki_page;
+--------------------------+--------------------------+
| min(char_length(`text`)) | max(char_length(`text`)) |
+--------------------------+--------------------------+
|                       16 |                    65532 |
+--------------------------+--------------------------+
```
The dataset can be download from https://dumps.wikimedia.org/backup-index.html, for example https://dumps.wikimedia.org/enwiki/20250620/enwiki-20250620-pages-articles.xml.bz2

### Generate data for TiDB

1. create table `fts`.`wiki_page`
```sql
CREATE TABLE `wiki_page` (
  `title` varchar(256) DEFAULT NULL,
  `text` text DEFAULT NULL,
  `comment` text DEFAULT NULL,
  `username` varchar(256) DEFAULT NULL,
  `timestamp` timestamp NULL DEFAULT NULL,
  `id` bigint NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
)
```
2. download data and decompress the files to `xml` dir
3. generate csv files
 ```bash
 ./fts -i xml -R 5000 -c fts.wiki_page --dataset enwiki-page
 ```
 4. use tidb-lightning import the csv files

