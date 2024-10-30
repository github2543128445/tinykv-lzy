#!/bin/bash

# 遍历所有生成的 .txt 文件
for file in ./out-50/*; do
    # 使用 perl 删除以 "|| true" 开头并以 "ok" 结尾的部分，包括跨行
    perl -0777 -i -pe 's/^\|\| true.*?ok\n//sg' "$file"
done