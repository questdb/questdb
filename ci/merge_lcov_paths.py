#!/usr/bin/env python3
"""
Merge LCOV file with duplicate entries caused by different build paths.
Normalizes paths and merges coverage data by taking maximum execution counts.
"""

import sys
import re


class LcovMerger:
    def __init__(self):
        self.files = {}  # normalized_path -> file data

    def normalize_path(self, path):
        """Remove CI-specific path prefixes"""
        # Remove /azp/_work/N/s/ prefix
        path = re.sub(r'/azp/_work/\d+/s/', '', path)
        return path

    def parse_and_merge_lcov(self, filepath):
        """Parse LCOV file and merge duplicate entries"""
        current_file = None
        current_file_data = None

        with open(filepath, 'r') as f:
            for line in f:
                line = line.rstrip()

                if line.startswith('SF:'):
                    # Start of a new file entry
                    raw_path = line[3:]
                    current_file = self.normalize_path(raw_path)

                    if current_file not in self.files:
                        self.files[current_file] = {
                            'functions': {},      # fn_name -> (line, count)
                            'lines': {},          # line -> count
                            'fn_defs': {},        # line -> fn_name
                            'branches': {}        # line:block -> taken,not_taken
                        }
                    current_file_data = self.files[current_file]

                elif line.startswith('FN:') and current_file_data is not None:
                    # Function definition: FN:line,name
                    parts = line[3:].split(',', 1)
                    if len(parts) == 2:
                        line_num, fn_name = parts
                        current_file_data['fn_defs'][line_num] = fn_name

                elif line.startswith('FNDA:') and current_file_data is not None:
                    # Function execution: FNDA:count,name
                    parts = line[5:].split(',', 1)
                    if len(parts) == 2:
                        count, fn_name = parts
                        current_count = current_file_data['functions'].get(fn_name, 0)
                        current_file_data['functions'][fn_name] = max(current_count, int(count))

                elif line.startswith('DA:') and current_file_data is not None:
                    # Line execution: DA:line,count
                    parts = line[3:].split(',')
                    if len(parts) >= 2:
                        line_num, count = parts[0], parts[1]
                        current_count = current_file_data['lines'].get(line_num, 0)
                        current_file_data['lines'][line_num] = max(current_count, int(count))

                elif line.startswith('BRDA:') and current_file_data is not None:
                    # Branch data: BRDA:line,block,branch,taken
                    parts = line[5:].split(',')
                    if len(parts) == 4:
                        line_num, block, branch, taken = parts
                        key = f"{line_num}:{block}:{branch}"
                        if taken != '-':
                            current_taken = current_file_data['branches'].get(key, 0)
                            current_file_data['branches'][key] = max(current_taken, int(taken))
                        else:
                            if key not in current_file_data['branches']:
                                current_file_data['branches'][key] = '-'

                elif line == 'end_of_record':
                    current_file = None
                    current_file_data = None

    def write_lcov(self, output_path):
        """Write merged data to LCOV file"""
        with open(output_path, 'w') as f:
            for filepath in sorted(self.files.keys()):
                data = self.files[filepath]

                f.write(f'SF:{filepath}\n')

                # Write function definitions
                for line_num in sorted(data['fn_defs'].keys(), key=lambda x: int(x)):
                    fn_name = data['fn_defs'][line_num]
                    f.write(f'FN:{line_num},{fn_name}\n')

                # Write function execution counts
                fns_hit = 0
                for fn_name in sorted(data['functions'].keys()):
                    count = data['functions'][fn_name]
                    f.write(f'FNDA:{count},{fn_name}\n')
                    if count > 0:
                        fns_hit += 1

                # Write function summary
                if data['functions']:
                    f.write(f'FNF:{len(data["functions"])}\n')
                    f.write(f'FNH:{fns_hit}\n')

                # Write line execution counts
                lines_hit = 0
                for line_num in sorted(data['lines'].keys(), key=lambda x: int(x)):
                    count = data['lines'][line_num]
                    f.write(f'DA:{line_num},{count}\n')
                    if count > 0:
                        lines_hit += 1

                # Write line summary
                if data['lines']:
                    f.write(f'LF:{len(data["lines"])}\n')
                    f.write(f'LH:{lines_hit}\n')

                # Write branch data
                branches_total = 0
                branches_hit = 0
                for key in sorted(data['branches'].keys()):
                    taken = data['branches'][key]
                    line_num, block, branch = key.split(':')
                    f.write(f'BRDA:{line_num},{block},{branch},{taken}\n')
                    branches_total += 1
                    if taken != '-' and int(taken) > 0:
                        branches_hit += 1

                # Write branch summary
                if branches_total > 0:
                    f.write(f'BRF:{branches_total}\n')
                    f.write(f'BRH:{branches_hit}\n')

                f.write('end_of_record\n')


def main():
    if len(sys.argv) != 3:
        print("Usage: merge_lcov_paths.py <input.lcov> <output.lcov>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    print(f"Reading {input_file}...")
    merger = LcovMerger()
    merger.parse_and_merge_lcov(input_file)

    print(f"Found {len(merger.files)} unique source files after normalization")

    print(f"Writing merged coverage to {output_file}...")
    merger.write_lcov(output_file)

    print("Done!")


if __name__ == '__main__':
    main()
