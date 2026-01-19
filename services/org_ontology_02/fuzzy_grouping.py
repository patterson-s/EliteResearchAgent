from rapidfuzz import fuzz
from typing import List, Tuple, Dict
from collections import defaultdict

def compute_similarity(str1: str, str2: str) -> int:
    return fuzz.token_sort_ratio(str1, str2)

def cluster_organizations(org_list: List[Tuple[str, int]], threshold: int = 80) -> List[List[Tuple[str, int]]]:
    clusters = []
    used = set()
    
    for i, (org1, count1) in enumerate(org_list):
        if org1 in used:
            continue
        
        cluster = [(org1, count1)]
        used.add(org1)
        
        for j, (org2, count2) in enumerate(org_list[i+1:], start=i+1):
            if org2 in used:
                continue
            
            similarity = compute_similarity(org1, org2)
            
            if similarity >= threshold:
                cluster.append((org2, count2))
                used.add(org2)
        
        clusters.append(cluster)
    
    clusters.sort(key=lambda c: sum(count for _, count in c), reverse=True)
    
    return clusters

def get_cluster_summary(cluster: List[Tuple[str, int]]) -> dict:
    total_count = sum(count for _, count in cluster)
    org_strings = [org for org, _ in cluster]
    
    return {
        'size': len(cluster),
        'total_mentions': total_count,
        'organizations': org_strings,
        'primary': cluster[0][0] if cluster else None
    }

if __name__ == '__main__':
    from load_data import get_organization_data, load_config
    
    config = load_config()
    org_list, _ = get_organization_data()
    
    clusters = cluster_organizations(org_list, config['fuzzy_threshold'])
    
    print(f"Total clusters: {len(clusters)}")
    print(f"\nTop 10 largest clusters:")
    for i, cluster in enumerate(clusters[:10], 1):
        summary = get_cluster_summary(cluster)
        print(f"\nCluster {i}: {summary['total_mentions']} mentions, {summary['size']} variants")
        print(f"  Primary: {summary['primary']}")
        if summary['size'] > 1:
            print(f"  Variants: {', '.join(summary['organizations'][1:])}")