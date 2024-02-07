from heterogeneous import gen_ra_from_events
from isomorphic import gen_rr_aa_from_ra
import networkx as nx
import os
import argparse  # 导入argparse模块


def green_print(text):
    print(f"\033[92m{text}\033[0m")


def get_org_files(events_dir):
    org_files = {}
    for org in os.listdir(events_dir):
        org_path = os.path.join(events_dir, org)
        if os.path.isdir(org_path):
            files = []
            for file_name in os.listdir(org_path):
                if file_name.endswith('.csv') and not file_name.startswith('.'):
                    files.append(file_name)
            org_files[org] = files
    return org_files


def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


# 设置argparse
parser = argparse.ArgumentParser(description='Process and analyze event files.')
parser.add_argument('--input_dir', type=str, default='events',
                    help='Input directory containing event files. Default is "events".')
parser.add_argument('--output_dir', type=str, default='networks',
                    help='Output directory for processed files. Default is "networks".')

args = parser.parse_args()

input_dir = args.input_dir
output_dir = args.output_dir

org_files = get_org_files(input_dir)
green_print(f'org_files: {org_files}')

for org, files in org_files.items():
    for file_name in files:
        input_file_path = os.path.join(input_dir, org, file_name)
        green_print(f'Processing {input_file_path}')

        # 确保输出目录存在
        org_output_dir = os.path.join(output_dir, org)
        ensure_dir(org_output_dir)

        green_print('Generating ra...')
        ra = gen_ra_from_events(input_file_path)

        green_print(f'write ra to gml file...')
        nx.write_gml(ra, os.path.join(org_output_dir, f"{file_name[:-4]}_ra.gml"))

        green_print(f'write ra to pajek file...')
        nx.write_pajek(ra, os.path.join(org_output_dir, f"{file_name[:-4]}_ra.net"))

        green_print('Generating rr and aa...')
        rr, aa = gen_rr_aa_from_ra(ra)

        green_print(f'write rr and aa to gml file...')
        nx.write_gml(rr, os.path.join(org_output_dir, f"{file_name[:-4]}_rr.gml"))
        nx.write_gml(aa, os.path.join(org_output_dir, f"{file_name[:-4]}_aa.gml"))

        green_print(f'write rr and aa to pajek file...')
        nx.write_pajek(rr, os.path.join(org_output_dir, f"{file_name[:-4]}_rr.net"))
        nx.write_pajek(aa, os.path.join(org_output_dir, f"{file_name[:-4]}_aa.net"))
