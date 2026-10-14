"""Recalculate published metrics and a small chart from the included CSV."""
from pathlib import Path
import json
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent

def main():
    data = pd.read_csv(ROOT / 'Data/cleaned_delivery_analysis.csv')
    assert data['order_id'].notna().all(), 'Missing order IDs'
    assert data['review_score'].between(1, 5).all(), 'Invalid review scores'
    assert data['actual_delivery_days'].ge(0).all(), 'Negative delivery durations'
    assert set(data['delivery_status']) == {'On-Time', 'Delayed'}, 'Unknown delivery status'
    assert data.groupby('order_id')['delivery_status'].nunique().le(1).all(), 'Conflicting order statuses'
    summary = data.groupby('delivery_status').agg(
        joined_records=('order_id', 'size'), unique_orders=('order_id', 'nunique'),
        average_review=('review_score', 'mean'), median_review=('review_score', 'median')
    ).reindex(['On-Time', 'Delayed'])
    summary['record_share'] = summary['joined_records'] / len(data)
    report = {
        'joined_records': len(data), 'unique_orders': int(data.order_id.nunique()),
        'purchase_date_start': data.purchase_date.min(), 'purchase_date_end': data.purchase_date.max(),
        'delay_rate_by_record': float(data.delivery_status.eq('Delayed').mean()),
        'delay_rate_by_order': float(data.drop_duplicates('order_id').delivery_status.eq('Delayed').mean()),
        'groups': summary.reset_index().to_dict('records'),
        'note': 'Joined records preserve multiple reviews per order; association does not prove causation.'
    }
    out = ROOT / 'reports'
    out.mkdir(exist_ok=True)
    (out / 'metrics.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
    colors = ['#087f8c', '#e07a32']
    for ax, col, title in zip(axes, ['joined_records', 'average_review'], ['Joined records by delivery status', 'Average customer review score']):
        bars = ax.bar(summary.index, summary[col], color=colors, width=.55)
        ax.set_title(title, loc='left', fontsize=13, fontweight='bold', pad=16)
        ax.spines[['top', 'right', 'left']].set_visible(False)
        ax.tick_params(length=0)
        ax.grid(axis='y', alpha=.13)
        ax.set_axisbelow(True)
        ax.set_ylim(0, 105000 if col == 'joined_records' else 5)
        for bar, value in zip(bars, summary[col]):
            ax.text(bar.get_x()+bar.get_width()/2, value+(1600 if col == 'joined_records' else .08), f'{value:,.0f}' if col == 'joined_records' else f'{value:.2f} / 5', ha='center', fontsize=11)
    fig.suptitle('Olist | Delivery and customer reviews', x=.06, ha='left', fontsize=19, fontweight='bold', color='#172b4d')
    fig.text(.06, .04, '96,353 joined records · 95,824 unique orders · Multiple reviews per order are retained.\nHistorical data; the comparison is descriptive and does not establish causation.', fontsize=9, color='#475569')
    fig.subplots_adjust(top=.76, bottom=.23, left=.06, right=.97, wspace=.24)
    fig.savefig(out / 'delivery_review_summary.png', dpi=170, facecolor='white')
    plt.close(fig)
    print(json.dumps(report, indent=2))

if __name__ == '__main__':
    main()
