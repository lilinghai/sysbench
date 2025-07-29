
# Use cases
## (一) Enwiki abstract

Enwiki-abstract [details here](https://en.wikipedia.org/wiki/Wikipedia:Database_download), from English-language Wikipedia:Database page abstracts.

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

## (二) Enwiki pages

Enwiki-pages [details here](https://en.wikipedia.org/wiki/Wikipedia:Database_download), from English-language Wikipedia:Database last page revisions, containing processed metadata extracted from the full Wikipedia XML dumppage abstracts.

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
The dataset can be download from https://dumps.wikimedia.org/backup-index.html  
For example, the [20250620 dataset](https://dumps.wikimedia.org/enwiki/20250620/enwiki-20250620-pages-articles.xml.bz2) size is about 22.4 GB, and has 25 million documents.

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


## (三) Amazon reviews

The Amazon-reviews [details here](https://amazon-reviews-2023.github.io/) dataset comprises about 130 million Amazon customer reviews. It is a few Snappy-compressed Parquet files with a total size of 37GB.

### Example document

```sql
select * from fts.amazon_review limit 1 \G
*************************** 1. row ***************************
      review_date: 16491
      marketplace: US
      customer_id: 45922458
        review_id: R36PNZWZFNFG63
       product_id: B00KPVCEDU
   product_parent: 137095701
    product_title: A Ghostly Undertaking: A Ghostly Southern Mystery (Ghostly Southern Mysteries Book 1)
 product_category: Digital_Ebook_Purchase
      star_rating: 4
    helpful_votes: 3
      total_votes: 3
             vine: 0
verified_purchase: 0
  review_headline: Kindle Copy for Review Emma Lee Raines runs the Eternal ...
      review_body: Kindle Copy for Review<br /><br />Emma Lee Raines runs the Eternal Slumber Funeral Home with her sister Charlotte as a funeral director.  What she thought was just another day and burial turns out to be more than meets the eye.<br /><br />Her grandmother enemy who shared an inn left by both former husband they shared.  Ruthie Sue Payne has decided to haunt Emma until she finds her killer.  What appeared to be a case of falling down the stairs as to her demise, she tells Emma she was pushed down the stairs as Emma’s grandmother found the body.<br /><br />As evidence seems to point to her grandmother, Emma must find the real killer or be haunted by Ruthie for the rest of her life.  When she gets too close, she becomes the next target in the game.  Can she find out the reason why Ruthie was murdered before it is too late?
               id: 9637712
```

### Document statistics

The size of csv file is about 150MB for 400000 rows

```sql
select avg(char_length(product_title)),avg(char_length(review_headline)),avg(char_length(review_body)) from amazon_review;
+---------------------------------+-----------------------------------+-------------------------------+
| avg(char_length(product_title)) | avg(char_length(review_headline)) | avg(char_length(review_body)) |
+---------------------------------+-----------------------------------+-------------------------------+
|                         63.5442 |                           23.0701 |                      199.8870 |
+---------------------------------+-----------------------------------+-------------------------------+
select min(char_length(`product_title`)), max(char_length(`product_title`)),min(char_length(`review_headline`)), max(char_length(`review_headline`)),min(char_length(`review_body`)), max(char_length(`review_body`)) from amazon_review;
+-----------------------------------+-----------------------------------+-------------------------------------+-------------------------------------+---------------------------------+---------------------------------+
| min(char_length(`product_title`)) | max(char_length(`product_title`)) | min(char_length(`review_headline`)) | max(char_length(`review_headline`)) | min(char_length(`review_body`)) | max(char_length(`review_body`)) |
+-----------------------------------+-----------------------------------+-------------------------------------+-------------------------------------+---------------------------------+---------------------------------+
|                                 1 |                               400 |                                   0 |                                 128 |                               0 |                           48087 |
+-----------------------------------+-----------------------------------+-------------------------------------+-------------------------------------+---------------------------------+---------------------------------+
```
datasets: Snappy-compressed Parquet files with a total size of 37GB  
[amazon_reviews_2010](https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2010.snappy.parquet)  
[amazon_reviews_2011](https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2011.snappy.parquet)  
[amazon_reviews_2012](https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2012.snappy.parquet)  
[amazon_reviews_2013](https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2013.snappy.parquet)  
[amazon_reviews_2014](https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2014.snappy.parquet)  
[amazon_reviews_2015](https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet)

### Generate data for TiDB

1. create table `fts`.`amazon_review`
```sql
CREATE TABLE `amazon_review` (
  `review_date` int DEFAULT NULL,
  `marketplace` varchar(20) DEFAULT NULL,
  `customer_id` bigint DEFAULT NULL,
  `review_id` varchar(40) DEFAULT NULL,
  `product_id` varchar(20) DEFAULT NULL,
  `product_parent` bigint DEFAULT NULL,
  `product_title` varchar(500) DEFAULT NULL,
  `product_category` varchar(50) DEFAULT NULL,
  `star_rating` int DEFAULT NULL,
  `helpful_votes` int DEFAULT NULL,
  `total_votes` int DEFAULT NULL,
  `vine` tinyint(1) DEFAULT NULL,
  `verified_purchase` tinyint(1) DEFAULT NULL,
  `review_headline` varchar(500) DEFAULT NULL,
  `review_body` text DEFAULT NULL,
  `id` bigint NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
)
```
2. download data and decompress the files to `parquet` dir
3. generate csv files
 ```bash
 ./fts -i parquet -R 400000 -c fts.amazon_review --dataset amazon-review
 ```
 4. use tidb-lightning import the csv files
