from mlb_stats.sensors import mlb_stats

sensors = [
    mlb_stats.trigger_schedule_on_raw_success,
    mlb_stats.trigger_box_score_on_raw_success
]

__all__ = [
    'sensors'
]
