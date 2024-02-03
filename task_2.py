import networkx as nx
import os
from itertools import combinations

input_ra_gml_path = 'output/repo_actor_network/xlab_ra.gml'
output_rr_gml_path = 'output/repo_repo_network/xlab_rr.gml'
output_aa_gml_path = 'output/actor_actor_network/xlab_aa.gml'
output_rr_pajek_path = 'output/repo_repo_network/xlab_rr.net'
output_aa_pajek_path = 'output/actor_actor_network/xlab_aa.net'

ratio = 0.1  # 用于调和平均的权重比例


def harmonic_mean(weights):
    """计算权重的调和平均值。"""
    return ratio * len(weights) / sum(1.0 / w for w in weights) if weights else 0


# 读取异质图
G = nx.read_gml(input_ra_gml_path)

# 初始化同质图
G_rr = nx.Graph()  # 使用无向图
G_aa = nx.Graph()  # 修改为无向图

# 构建repo_repo图
actor_to_repos = {}
for u, v, data in G.edges(data=True):
    if G.nodes[u]['type'] == 'actor' and G.nodes[v]['type'] == 'repo':
        actor_to_repos.setdefault(u, []).append((v, data['weight']))
    if G.nodes[u]['type'] == 'repo' and G.nodes[v]['type'] == 'repo':
        base_weight = 0
        if G_rr.has_edge(u, v):
            base_weight = G_rr[u][v]['weight']
        G_rr.add_edge(u, v, weight=base_weight + data['weight'])

for actor, repos_weights in actor_to_repos.items():
    for (repo1, weight1), (repo2, weight2) in combinations(repos_weights, 2):
        weight = harmonic_mean([weight1, weight2])
        if G_rr.has_edge(repo1, repo2):
            G_rr[repo1][repo2]['weight'] += weight
        else:
            G_rr.add_edge(repo1, repo2, weight=weight)

# 构建actor_actor图
# 初始化actor之间共同repo的权重累加
actor_pairs = {}

# 构建actor到repo的边权重映射
actor_to_repo_weights = {}
for u, v, data in G.edges(data=True):
    if G.nodes[u]['type'] == 'actor' and G.nodes[v]['type'] == 'repo':
        actor_to_repo_weights.setdefault(u, {}).setdefault(v, 0)
        actor_to_repo_weights[u][v] += data['weight']  # 累加权重，以处理多条边的情况
    if G.nodes[u]['type'] == 'actor' and G.nodes[v]['type'] == 'actor':
        base_weight = 0
        if G_aa.has_edge(u, v):
            base_weight = G_aa[u][v]['weight']
        G_aa.add_edge(u, v, weight=base_weight + data['weight'])

# 计算共同repo权重的调和平均并累加到actor之间的权重
for actor1, repos_weights1 in actor_to_repo_weights.items():
    for actor2, repos_weights2 in actor_to_repo_weights.items():
        if actor1 != actor2:
            common_repos = set(repos_weights1.keys()) & set(repos_weights2.keys())
            total_weight = sum(harmonic_mean([repos_weights1[repo], repos_weights2[repo]]) for repo in common_repos)
            if total_weight > 0:
                # 检查是否已经存在mention关系的基础权重
                base_weight = 0
                if G_aa.has_edge(actor1, actor2):
                    base_weight = G_aa[actor1][actor2]['weight']
                # 更新权重
                G_aa.add_edge(actor1, actor2, weight=base_weight + total_weight)
# 保存图到文件
nx.write_gml(G_rr, output_rr_gml_path)
nx.write_gml(G_aa, output_aa_gml_path)
nx.write_pajek(G_rr, output_rr_pajek_path)
nx.write_pajek(G_aa, output_aa_pajek_path)

print("构建完成，并已保存到文件。")
