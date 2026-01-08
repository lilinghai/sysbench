#!/usr/bin/env sysbench

require("oltp_common")
local csv = require("csv")

function prepare_statements()
    -- We do not use prepared statements here, but oltp_common.sh expects this
    -- function to be defined
end

function insert_row(row)
    if sysbench.opt.workload == "wiki_abstract" then
        -- abstract,title,url
        con:query(string.format("INSERT INTO wiki_abstract (abstract,title,url) VALUES " .. "('%s', '%s', '%s')",
            row["abstract"], row["title"], row["url"]))
    elseif sysbench.opt.workload == "wiki_page" then
        -- title,text,comment,username,timestamp
        con:query(string.format("INSERT INTO wiki_page (title,`text`,`comment`,username,`timestamp`) VALUES " ..
                                    "('%s', '%s', '%s','%s', '%s')", row["title"], row["text"], row["comment"],
            row["username"], row["timestamp"]))
    elseif sysbench.opt.workload == "amazon_review" then
        -- review_date,marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body
        con:query(string.format(
            "INSERT INTO wiki_page (review_date,marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body) VALUES " ..
                "('%d','%s','%d', '%s', '%s','%d','%s', '%s',,'%d','%d','%d','%d','%d','%s', '%s')", row["review_date"],
            row["marketplace"], row["customer_id"], row["review_id"], row["product_id"], row["product_parent"],
            row["product_title"], row["product_category"], row["star_rating"], row["helpful_votes"], row["total_votes"],
            row["vine"], row["verified_purchase"], row["review_headline"], row["review_body"]))
    end
end

-- it means the sysbench report is meanless
function event()
    local file_name = "fts.wiki_abstract"
    if sysbench.opt.workload == "wiki_abstract" then
        file_name = "fts.wiki_abstract"
    elseif sysbench.opt.workload == "wiki_page" then
        file_name = "fts.wiki_page"
    elseif sysbench.opt.workload == "amazon_review" then
        file_name = "fts.amazon_review"
    else
        error("Unknown workload: " .. sysbench.opt.workload)
    end
    -- file name format: fts.wiki_abstract.3.csv
    file_name = file_name .. "." .. sysbench.rand.uniform(1, sysbench.opt.source_files) .. ".csv"
    local f = csv.open(file_name, {
        header = true
    })
    print("Thread ", sysbench.tid, " open csv file name: ", file_name)
    --  iter read for large file
    for r in f:lines() do
        for k, v in pairs(r) do
            v = string.gsub(v, "\\", "")
            v = string.gsub(v, "([\"'])", "\\%1")
            r[k] = v
        end
        insert_row(r)
    end
    f:close()
    check_reconnect()
end
