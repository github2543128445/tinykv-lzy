#!/bin/bash

# 定义一个函数来检查文件中是否包含“FAIL”
check_fail() {
    if grep -q "FAIL" "$1"; then
        return 1
    else
        return 0
    fi
}

rm -rf ./out/*

for ((i=1,j=50;i<=j;i++)); do
    echo "ROUND $i/$j"
    # 修改下方进行你想要的测试
    make projectAll > ./out/out-$i.txt
    # 检查文件中是否包含“FAIL”
    if check_fail "./out/out-$i.txt"; then
        # 如果没有“FAIL”，删除文件
        rm ./out/out-$i.txt
		echo "pass: out-$i.txt" >> ./out/finish.txt
    else
        # 如果有“FAIL”，将文件名追加到 finish.txt
        echo "fail: out-$i.txt" >> ./out/finish.txt
    fi
done

echo "测试完成, 结果已保存到 finish.txt"