#!/bin/bash

# 检查是否提供了目录参数
if [ "$#" -ne 1 ]; then
    echo "使用方法: $0 /path/to/directory"
    exit 1
fi

# 目录参数
directory=$1

# 检查目录是否存在
if [ ! -d "$directory" ]; then
    echo "错误：目录不存在 - $directory"
    exit 1
fi

# 进入指定目录
cd "$directory"

# 查找所有以-schema.sql.xz结尾的文件
files=$(find . -type f -name '*-schema.sql.xz')

# 遍历找到的文件
for file in $files; do
    echo "处理文件: $file"

    # 解压缩文件
    xz -d "$file"

    # 获取不包含.xz扩展名的文件名
    filename="${file%.sql.xz}.sql"

    # 修改存储引擎
    sed -i 's/ ENGINE=InnoDB/ ENGINE=MyISAM/' "$filename"

    # 检查sed命令是否成功执行
    if [ $? -eq 0 ]; then
        echo "存储引擎已修改为MyISAM: $filename"

        # 重新压缩文件
        xz "$filename"

        # 检查压缩是否成功
        if [ $? -eq 0 ]; then
            echo "文件已重新压缩: $file"
        else
            echo "文件压缩失败: $file"
        fi
    else
        echo "存储引擎修改失败: $file"
    fi
done

echo "所有文件处理完毕。"