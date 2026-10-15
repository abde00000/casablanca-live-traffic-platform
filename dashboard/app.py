
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from deltalake import DeltaTable
from streamlit_autorefresh import st_autorefresh


ROOT = Path(__file__).resolve().parents[1]

GOLD_PATH = Path(
    os.getenv(
        "GOLD_DELTA_PATH",
        str(
            ROOT
            / "data"
            / "delta"
            / "gold"
            / "traffic_hourly_by_arrondissement"
        ),
    )
)

LOCATIONS_PATH = Path(
    os.getenv(
        "TRAFFIC_LOCATIONS_FILE",
        str(ROOT / "config" / "traffic_locations.csv"),
    )
)

st.set_page_config(
    page_title="Casablanca Traffic Dashboard",
    page_icon="🚦",
    layout="wide",
)


@st.cache_data(ttl=30, show_spinner=False)
def read_gold(path: str) -> pd.DataFrame:
    """Read the latest snapshot of the Spark Gold Delta table."""
    table = DeltaTable(path)
    return table.to_pandas()


@st.cache_data(ttl=300, show_spinner=False)
def read_locations(path: str) -> pd.DataFrame:
    """Read sample-point coordinates."""
    locations = pd.read_csv(path)

    for col in ("latitude", "longitude"):
        if col in locations.columns:
            locations[col] = pd.to_numeric(
                locations[col], errors="coerce"
            )

    return locations


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Gold data types for filtering and visualization."""
    if df.empty or "hour_start_utc" not in df.columns:
        return pd.DataFrame()

    df = df.copy()

    df["hour_start_utc"] = pd.to_datetime(
        df["hour_start_utc"],
        utc=True,
        errors="coerce",
    )

    df = df.dropna(subset=["hour_start_utc"])

    numeric_columns = [
        "avg_current_speed_kmh",
        "avg_free_flow_speed_kmh",
        "avg_speed_ratio",
        "avg_travel_time_index",
        "avg_delay_seconds",
        "avg_delay_pct",
        "avg_provider_confidence",
        "observation_count",
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.sort_values("hour_start_utc")


def main() -> None:
    # Refresh every minute. New data still depend on the pipeline.
    st_autorefresh(
        interval=60_000,
        key="casablanca-traffic-refresh",
    )

    st.title("Casablanca Traffic Dashboard")

    st.caption(
        "Hourly summaries from the Gold Delta table. "
        "The dashboard refreshes every minute when new data are available."
    )

    # Load Gold data.
    if not GOLD_PATH.exists():
        st.error(
            "The Gold Delta table was not found at:\n\n"
            f"{GOLD_PATH}"
        )
        st.stop()

    try:
        raw = read_gold(str(GOLD_PATH))
    except Exception as exc:
        st.error("Could not read the Gold Delta table.")
        st.caption(f"Technical detail: {exc}")
        st.stop()

    df = prepare_data(raw)

    if df.empty:
        st.info(
            "The Gold table contains no usable hourly records yet."
        )
        st.stop()

    required_columns = {"arrondissement", "location_id"}

    if not required_columns.issubset(df.columns):
        st.error(
            "The Gold table is missing required location columns."
        )
        st.stop()

    # Sidebar filters.
    latest_time = df["hour_start_utc"].max()
    min_time = df["hour_start_utc"].min()

    days_available = max(
        1,
        (latest_time.date() - min_time.date()).days + 1,
    )

    st.sidebar.header("Dashboard filters")

    max_days = min(30, max(1, days_available))

    if max_days == 1:
        days_to_show = 1
        st.sidebar.caption(
            "Showing the only available day of data."
        )
    else:
        days_to_show = st.sidebar.slider(
            "Days to show",
            min_value=1,
            max_value=max_days,
            value=min(7, max_days),
        )

    area_options = ["All arrondissements"] + sorted(
        str(x)
        for x in df["arrondissement"].dropna().unique()
    )

    selected_area = st.sidebar.selectbox(
        "Arrondissement",
        area_options,
    )

    cutoff = latest_time - pd.Timedelta(days=days_to_show)

    period_df = df[
        df["hour_start_utc"] >= cutoff
    ].copy()

    if selected_area != "All arrondissements":
        selected_df = period_df[
            period_df["arrondissement"].astype(str)
            == selected_area
        ].copy()
    else:
        selected_df = period_df.copy()

    if selected_df.empty:
        st.warning(
            "No records match the selected filters."
        )
        st.stop()

    # Latest available observations.
    latest_period_time = selected_df["hour_start_utc"].max()

    latest_rows = selected_df[
        selected_df["hour_start_utc"] == latest_period_time
    ]

    def mean_or_nan(column: str) -> float:
        if column not in latest_rows.columns:
            return float("nan")
        return latest_rows[column].mean()

    avg_speed = mean_or_nan("avg_current_speed_kmh")
    avg_tti = mean_or_nan("avg_travel_time_index")
    avg_delay = mean_or_nan("avg_delay_seconds")
    avg_conf = mean_or_nan("avg_provider_confidence")

    if "observation_count" in latest_rows.columns:
        obs_total = latest_rows["observation_count"].sum()
    else:
        obs_total = 0

    # KPI cards.
    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric(
        "Latest sample speed",
        f"{avg_speed:.1f} km/h" if pd.notna(avg_speed) else "—",
    )

    c2.metric(
        "Latest travel-time index",
        f"{avg_tti:.2f}" if pd.notna(avg_tti) else "—",
    )

    c3.metric(
        "Latest estimated delay",
        f"{avg_delay:.0f} sec" if pd.notna(avg_delay) else "—",
    )

    c4.metric(
        "Observations in latest hour",
        f"{obs_total:,.0f}",
    )

    c5.metric(
        "Average provider confidence",
        f"{avg_conf:.2f}" if pd.notna(avg_conf) else "—",
    )

    st.caption(
        "The travel-time index (TTI) compares current travel time "
        "with the provider's free-flow estimate. A value above 1 "
        "indicates a longer estimated travel time. Results describe "
        "sampled road points, not every road in Casablanca."
    )

    # Time-series and arrondissement comparison.
    left, right = st.columns((1.5, 1))

    with left:
        st.subheader("Travel-time index over time")

        if selected_area == "All arrondissements":
            line_df = (
                period_df.groupby(
                    "hour_start_utc",
                    as_index=False,
                )
                .agg(
                    avg_travel_time_index=(
                        "avg_travel_time_index",
                        "mean",
                    )
                )
                .dropna(
                    subset=["avg_travel_time_index"]
                )
            )

            if line_df.empty:
                st.info("No TTI data are available for this period.")
            else:
                line_df["series"] = "Average of sampled points"

                fig = px.line(
                    line_df,
                    x="hour_start_utc",
                    y="avg_travel_time_index",
                    color="series",
                    markers=True,
                    labels={
                        "hour_start_utc": "Hour (UTC)",
                        "avg_travel_time_index": "Average TTI",
                        "series": "",
                    },
                    template="plotly_white",
                )

                fig.update_layout(
                    legend_title_text="",
                    margin=dict(l=10, r=10, t=20, b=10),
                )

                st.plotly_chart(
                    fig,
                    use_container_width=True,
                )

        else:
            line_df = selected_df.dropna(
                subset=["avg_travel_time_index"]
            )

            if line_df.empty:
                st.info("No TTI data are available for this arrondissement.")
            else:
                fig = px.line(
                    line_df,
                    x="hour_start_utc",
                    y="avg_travel_time_index",
                    color="arrondissement",
                    markers=True,
                    hover_data=[
                        "location_id",
                        "observation_count",
                    ],
                    labels={
                        "hour_start_utc": "Hour (UTC)",
                        "avg_travel_time_index": "Average TTI",
                        "arrondissement": "Arrondissement",
                    },
                    template="plotly_white",
                )

                fig.update_layout(
                    legend_title_text="",
                    margin=dict(l=10, r=10, t=20, b=10),
                )

                st.plotly_chart(
                    fig,
                    use_container_width=True,
                )

    with right:
        st.subheader("Average delay by arrondissement")

        bar_df = (
            period_df.groupby(
                "arrondissement",
                as_index=False,
            )
            .agg(
                avg_delay_seconds=(
                    "avg_delay_seconds",
                    "mean",
                ),
                observations=(
                    "observation_count",
                    "sum",
                ),
            )
            .dropna(subset=["avg_delay_seconds"])
            .sort_values(
                "avg_delay_seconds",
                ascending=True,
            )
        )

        if bar_df.empty:
            st.info("No delay data are available for this period.")
        else:
            fig = px.bar(
                bar_df,
                x="avg_delay_seconds",
                y="arrondissement",
                orientation="h",
                color="avg_delay_seconds",
                hover_data=["observations"],
                labels={
                    "avg_delay_seconds": "Mean estimated delay (seconds)",
                    "arrondissement": "Arrondissement",
                    "observations": "Observations",
                },
                color_continuous_scale="Teal",
                template="plotly_white",
            )

            fig.update_layout(
                showlegend=False,
                coloraxis_showscale=False,
                margin=dict(l=10, r=10, t=20, b=10),
            )

            st.plotly_chart(
                fig,
                use_container_width=True,
            )

    # Latest value at each sampled road point.
    st.subheader("Latest available value at each sample point")

    latest_by_location = (
        df.sort_values("hour_start_utc")
        .groupby("location_id", as_index=False)
        .tail(1)
        .copy()
    )

    try:
        locations = read_locations(str(LOCATIONS_PATH))

        if {
            "location_id",
            "latitude",
            "longitude",
        }.issubset(locations.columns):
            latest_by_location = latest_by_location.merge(
                locations[
                    ["location_id", "latitude", "longitude"]
                ],
                on="location_id",
                how="left",
            )
    except Exception as exc:
        st.caption(
            f"Location coordinates could not be loaded: {exc}"
        )

    if {
        "latitude",
        "longitude",
        "avg_travel_time_index",
    }.issubset(latest_by_location.columns):

        points = latest_by_location.dropna(
            subset=[
                "latitude",
                "longitude",
                "avg_travel_time_index",
            ]
        )

        if not points.empty:
            hover_data = {
                "location_id": True,
                "avg_travel_time_index": ":.2f",
                "hour_start_utc": True,
                "longitude": ":.5f",
                "latitude": ":.5f",
            }

            if "avg_current_speed_kmh" in points.columns:
                hover_data["avg_current_speed_kmh"] = ":.1f"

            fig = px.scatter(
                points,
                x="longitude",
                y="latitude",
                color="avg_travel_time_index",
                size=(
                    "observation_count"
                    if "observation_count" in points.columns
                    else None
                ),
                hover_name="arrondissement",
                hover_data=hover_data,
                labels={
                    "longitude": "Longitude",
                    "latitude": "Latitude",
                    "avg_travel_time_index": "TTI",
                },
                color_continuous_scale="Teal",
                template="plotly_white",
            )

            fig.update_yaxes(
                scaleanchor="x",
                scaleratio=1,
            )

            fig.update_layout(
                margin=dict(l=10, r=10, t=20, b=10),
                height=420,
            )

            st.plotly_chart(
                fig,
                use_container_width=True,
            )
        else:
            st.info(
                "Location coordinates are unavailable for the latest records."
            )
    else:
        st.info(
            "The location file must contain location_id, latitude, and longitude."
        )

    # Hourly records table.
    display_cols = [
        "hour_start_utc",
        "arrondissement",
        "road_name_osm",
        "avg_current_speed_kmh",
        "avg_travel_time_index",
        "avg_delay_seconds",
        "avg_provider_confidence",
        "observation_count",
    ]

    display_cols = [
        col for col in display_cols
        if col in selected_df.columns
    ]

    st.subheader("Hourly records")

    st.dataframe(
        selected_df.sort_values(
            "hour_start_utc",
            ascending=False,
        )[display_cols],
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "Last available hourly record: "
        f"{latest_time.strftime('%Y-%m-%d %H:%M UTC')}  |  "
        f"Source: {GOLD_PATH}"
    )


if __name__ == "__main__":
    main()
