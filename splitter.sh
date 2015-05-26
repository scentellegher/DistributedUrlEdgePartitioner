#!/usr/bin/bash

file="$1"
num_files="$2"

#lines per file.

total_lines=$(wc -l <${file})
((lines_per_file = (total_lines + num_files - 1) / num_files))

# Split the actual file, maintaining lines.

split -da 4 --lines=${lines_per_file} ${file} part

# Debug information

echo "Total lines     = ${total_lines}"
echo "Lines  per file = ${lines_per_file}"    
