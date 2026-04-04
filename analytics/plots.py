import base64
from typing import List

import plotly.express as px


def _fig_to_base64(fig) -> str:
    image_bytes = fig.to_image(format="png", width=1000, height=550, scale=2)
    return base64.b64encode(image_bytes).decode("utf-8")


def bar_failed_logins(users: List[str], attempts: List[int]) -> str:
    if not users:
        fig = px.bar(x=["No Data"], y=[0], title="Failed Login Attempts Per User")
        fig.update_layout(template="plotly_white")
        return _fig_to_base64(fig)

    fig = px.bar(
        x=users,
        y=attempts,
        labels={"x": "User", "y": "Failed Attempts"},
        title="Failed Login Attempts Per User",
        color=attempts,
        color_continuous_scale="Sunsetdark",
    )
    fig.update_layout(template="plotly_white")
    return _fig_to_base64(fig)


def line_attacks_over_time(days: List[str], counts: List[int]) -> str:
    if not days:
        fig = px.line(x=["No Data"], y=[0], title="Daily Attack Trends")
        fig.update_layout(template="plotly_white")
        return _fig_to_base64(fig)

    fig = px.line(
        x=days,
        y=counts,
        markers=True,
        labels={"x": "Date", "y": "Attack Count"},
        title="Daily Attack Trends",
    )
    fig.update_traces(line=dict(width=3))
    fig.update_layout(template="plotly_white")
    return _fig_to_base64(fig)


def pie_attack_types(labels: List[str], values: List[int]) -> str:
    if not labels:
        fig = px.pie(names=["No Data"], values=[1], title="Attack Types Distribution", hole=0.35)
        fig.update_layout(template="plotly_white")
        return _fig_to_base64(fig)

    fig = px.pie(
        names=labels,
        values=values,
        title="Attack Types Distribution",
        hole=0.35,
    )
    fig.update_layout(template="plotly_white")
    return _fig_to_base64(fig)
