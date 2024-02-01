# Description: 从GitHub事件数据中构建一个有向图，其中包含了actor和repo节点，以及贡献类和引用类边。

import pandas as pd
import networkx as nx
import re

file_path = 'input/github_events_xlab.csv'
chunksize = 10000  # 根据您的内存限制调整
G = nx.DiGraph()

# 初始化存储actors和repos信息的字典
actors_info = {}
repos_info = {}

print("开始收集节点信息...")
for chunk in pd.read_csv(file_path, chunksize=chunksize,
                         usecols=['actor_id', 'actor_login', 'repo_id', 'repo_name', 'org_id', 'org_login',
                                  'created_at']):
    for _, row in chunk.iterrows():
        actor_id, repo_id = str(row['actor_id']), str(row['repo_id'])
        created_at = pd.to_datetime(row['created_at'])

        # 更新actor信息，如果这是一个更晚的记录
        if actor_id not in actors_info or created_at > actors_info[actor_id]['latest']:
            actors_info[actor_id] = {
                'login': row['actor_login'],
                'latest': created_at
            }

        # 更新repo信息，如果这是一个更晚的记录
        if repo_id not in repos_info or created_at > repos_info[repo_id]['latest']:
            repos_info[repo_id] = {
                'name': row['repo_name'],
                'org_id': str(row['org_id']),
                'org_login': row['org_login'],
                'latest': created_at
            }
    print(f"已处理 {chunk.shape[0]} 行数据...")

# 创建节点，不包括'latest'属性
for actor_id, attrs in actors_info.items():
    G.add_node(actor_id, type='actor', login=attrs['login'])

for repo_id, attrs in repos_info.items():
    G.add_node(repo_id, type='repo', name=attrs['name'], org_id=attrs['org_id'], org_login=attrs['org_login'])

print(f"总共收集到 {len(actors_info)} 个独特的 actor 节点和 {len(repos_info)} 个独特的 repo 节点。")


def determine_contribution_weight(event_type, action, pull_merged):
    weights = {
        'IssueCommentEvent': 1,
        'IssuesEvent': 2 if action == 'opened' else 0,
        'PullRequestEvent': 3 if action == 'opened' else 0,
        'PullRequestReviewCommentEvent': 4,
        'PullRequestMerged': 5 if pull_merged == 1 else 0
    }
    return weights.get(event_type, 0)


def extract_references(body):
    if pd.isna(body):
        return [], []
    mention_pattern = r"@([a-zA-Z0-9_-]+)"
    repo_ref_pattern = r"github\.com/([a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+)"
    mentions = re.findall(mention_pattern, body)
    repo_refs = re.findall(repo_ref_pattern, body)
    return mentions, repo_refs


print("开始处理边的添加...")
# 我们需要创建一个从 login 到 actor_id 的映射
login_to_actor_id = {info['login']: actor_id for actor_id, info in actors_info.items()}
# 我们需要创建一个从 repo_name 到 repo_id 的映射
name_to_repo_id = {info['name']: repo_id for repo_id, info in repos_info.items()}

for chunk in pd.read_csv(file_path, chunksize=chunksize):
    for index, row in chunk.iterrows():
        # 在这里添加逻辑来处理每行数据，包括添加贡献类边和引用类边
        # 确保在添加边之前，边的两端节点都已经存在于G中

        # 添加贡献边
        actor_id, repo_id = str(row['actor_id']), str(row['repo_id'])
        weight = determine_contribution_weight(row['type'], row['action'], row['pull_merged'])
        if weight > 0:
            # 如果边已存在，更新权重
            if G.has_edge(actor_id, repo_id):
                G[actor_id][repo_id]['weight'] += weight
            else:
                G.add_edge(actor_id, repo_id, weight=weight, type='contribution')

        # 添加引用边
        mentions, repo_refs = extract_references(row['body'] if pd.notna(row['body']) else "")
        actor_id_str = str(row['actor_id'])  # 将actor_id转换为字符串
        # 添加mention边（actor到actor）
        for mention in mentions:
            if mention in login_to_actor_id:
                mentioned_actor_id_str = login_to_actor_id[mention]
                # 检查是否避免自引用且两个节点都存在于G中
                if actor_id_str != mentioned_actor_id_str and G.has_node(actor_id_str) and G.has_node(mentioned_actor_id_str):
                    if G.has_edge(actor_id_str, mentioned_actor_id_str):
                        G[actor_id_str][mentioned_actor_id_str]['weight'] += 2  # 累计权重
                    else:
                        G.add_edge(actor_id_str, mentioned_actor_id_str, weight=2, type='mention')

        # 添加repo引用边（repo到repo）
        repo_id_str = str(row['repo_id'])  # 将repo_id转换为字符串
        for repo_ref in repo_refs:
            referenced_repo_name = '/'.join(repo_ref.split('/')[-2:])  # 提取完整的repo_name
            referenced_repo_id = chunk[chunk['repo_name'] == referenced_repo_name]['repo_id'].unique()
            if referenced_repo_name in name_to_repo_id:
                referenced_repo_id_str = name_to_repo_id[referenced_repo_name]
                # 检查是否避免自引用且两个节点都存在于G中
                if repo_id_str != referenced_repo_id_str and G.has_node(repo_id_str) and G.has_node(referenced_repo_id_str):
                    if G.has_edge(repo_id_str, referenced_repo_id_str):
                        G[repo_id_str][referenced_repo_id_str]['weight'] += 3  # 累计权重
                    else:
                        G.add_edge(repo_id_str, referenced_repo_id_str, weight=3, type='repo_reference')

    print(f"已处理 {chunk.shape[0]} 行数据...")

contribution_edges_count = sum(1 for _, _, edge_data in G.edges(data=True) if edge_data.get('type') == 'contribution')
mention_edges_count = sum(1 for _, _, edge_data in G.edges(data=True) if edge_data.get('type') == 'mention')
repo_reference_edges_count = sum(1 for _, _, edge_data in G.edges(data=True) if edge_data.get('type') == 'repo_reference')
print(f"总共收集到 {contribution_edges_count} 个 contribution 边、 {mention_edges_count} 个 mention 边和 {repo_reference_edges_count} 个 repo_reference 边。")

# 转换所有属性为字符串类型
for node, attr in G.nodes(data=True):
    for key in attr:
        attr[key] = str(attr[key])
for u, v, attr in G.edges(data=True):
    for key in attr:
        attr[key] = str(attr[key])

# 导出到GML和Pajek格式
gml_file_path = 'output/repo_actor_network/xlab.gml'
pajek_file_path = 'output/repo_actor_network/xlab.net'
nx.write_gml(G, gml_file_path)
nx.write_pajek(G, pajek_file_path)
print("图数据已成功导出。")
