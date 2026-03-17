from pathlib import Path

import pandas as pd
import streamlit as st

PAYMENT_LABELS = {
    1: "Cartao de Credito",
    2: "Dinheiro",
    3: "Sem Cobranca",
    4: "Disputa",
    5: "Desconhecido",
    6: "Cancelada",
}

DAY_LABELS = {
    "Monday": "Segunda-feira",
    "Tuesday": "Terca-feira",
    "Wednesday": "Quarta-feira",
    "Thursday": "Quinta-feira",
    "Friday": "Sexta-feira",
    "Saturday": "Sabado",
    "Sunday": "Domingo",
}

DAY_ORDER = [
    "Segunda-feira",
    "Terca-feira",
    "Quarta-feira",
    "Quinta-feira",
    "Sexta-feira",
    "Sabado",
    "Domingo",
]

PARQUET_PATH = (
    Path(__file__).resolve().parent / "data" / "output" / "yellow_taxi_2016-03.parquet"
)


@st.cache_data(show_spinner="Lendo dados do Parquet...")
def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df["payment_label"] = df["payment_type"].map(PAYMENT_LABELS).fillna("Outro")
    df["day_of_week"] = df["day_of_week"].replace(DAY_LABELS)
    return df


def filter_data(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filtros")

    hour_range = st.sidebar.slider("Faixa de hora", 0, 23, (0, 23))

    vendor_options = sorted(df["VendorID"].unique().tolist())
    selected_vendors = st.sidebar.multiselect(
        "Fornecedores",
        options=vendor_options,
        default=vendor_options,
    )

    payment_options = sorted(df["payment_label"].unique().tolist())
    selected_payments = st.sidebar.multiselect(
        "Tipo de pagamento",
        options=payment_options,
        default=payment_options,
    )

    return df[
        df["hour_of_day"].between(hour_range[0], hour_range[1])
        & df["VendorID"].isin(selected_vendors)
        & df["payment_label"].isin(selected_payments)
    ]


def format_currency(value: float) -> str:
    value_str = f"{value:,.2f}".replace(",", "#").replace(".", ",").replace("#", ".")
    return f"US$ {value_str}"


def format_int(value: int) -> str:
    return f"{value:,}".replace(",", ".")


def main() -> None:
    st.set_page_config(
        page_title="NYC Yellow Taxi - Painel ETL",
        page_icon="🚕",
        layout="wide",
    )

    st.title("🚕 Dashboard ETL em Lote - NYC Yellow Taxi")
    st.markdown(
        """
        Projeto de Engenharia de Dados com fluxo ETL em lote (batch):
        **Extração -> Validação -> Tratamento -> Transformação -> Carga (Parquet) -> Análise**.
        """
    )

    df = load_data(PARQUET_PATH)
    filtered_df = filter_data(df)

    if filtered_df.empty:
        st.warning("Nenhum dado encontrado para os filtros selecionados.")
        st.stop()

    st.subheader("Resumo Geral")
    col1, col2, col3, col4, col5 = st.columns(5)

    total_trips = len(filtered_df)
    total_revenue = float(filtered_df["total_amount"].sum())
    avg_fare = float(filtered_df["fare_amount"].mean())
    avg_distance = float(filtered_df["trip_distance"].mean())
    avg_tip_pct = float(filtered_df["tip_pct"].mean())

    col1.metric("Viagens", format_int(total_trips))
    col2.metric("Receita total", format_currency(total_revenue))
    col3.metric("Tarifa média", format_currency(avg_fare))
    col4.metric("Distância média (mi)", f"{avg_distance:.2f}")
    col5.metric("Gorjeta média (%)", f"{avg_tip_pct:.2f}")

    tab_hour, tab_vendor, tab_payment, tab_weekday = st.tabs(
        ["Por Hora", "Por Fornecedor", "Por Pagamento", "Por Dia da Semana"]
    )

    with tab_hour:
        hourly = (
            filtered_df.groupby("hour_of_day", as_index=True)
            .agg(
                total_trips=("hour_of_day", "size"),
                avg_distance=("trip_distance", "mean"),
                avg_fare=("fare_amount", "mean"),
                avg_duration=("trip_duration_min", "mean"),
                avg_speed=("trip_speed_mph", "mean"),
                avg_tip_pct=("tip_pct", "mean"),
            )
            .sort_index()
            .round(2)
            .rename(
                columns={
                    "total_trips": "Total de viagens",
                    "avg_distance": "Distancia media (mi)",
                    "avg_fare": "Tarifa media (US$)",
                    "avg_duration": "Duracao media (min)",
                    "avg_speed": "Velocidade media (mph)",
                    "avg_tip_pct": "Gorjeta media (%)",
                }
            )
        )

        chart_col1, chart_col2 = st.columns(2)
        chart_col1.line_chart(hourly["Total de viagens"], use_container_width=True)
        chart_col2.bar_chart(hourly["Tarifa media (US$)"], use_container_width=True)
        st.dataframe(hourly, use_container_width=True)

    with tab_vendor:
        vendor = (
            filtered_df.groupby("VendorID", as_index=True)
            .agg(
                total_trips=("VendorID", "size"),
                avg_distance=("trip_distance", "mean"),
                avg_fare=("fare_amount", "mean"),
                total_revenue=("total_amount", "sum"),
                avg_tip_pct=("tip_pct", "mean"),
                avg_speed=("trip_speed_mph", "mean"),
            )
            .round(2)
            .rename(
                columns={
                    "total_trips": "Total de viagens",
                    "avg_distance": "Distancia media (mi)",
                    "avg_fare": "Tarifa media (US$)",
                    "total_revenue": "Receita total (US$)",
                    "avg_tip_pct": "Gorjeta media (%)",
                    "avg_speed": "Velocidade media (mph)",
                }
            )
        )

        st.bar_chart(vendor["Total de viagens"], use_container_width=True)
        st.dataframe(vendor, use_container_width=True)

    with tab_payment:
        payment = (
            filtered_df.groupby("payment_label", as_index=True)
            .agg(
                total_trips=("payment_label", "size"),
                avg_fare=("fare_amount", "mean"),
                avg_tip=("tip_amount", "mean"),
                avg_tip_pct=("tip_pct", "mean"),
                total_revenue=("total_amount", "sum"),
            )
            .round(2)
            .rename(
                columns={
                    "total_trips": "Total de viagens",
                    "avg_fare": "Tarifa media (US$)",
                    "avg_tip": "Gorjeta media (US$)",
                    "avg_tip_pct": "Gorjeta media (%)",
                    "total_revenue": "Receita total (US$)",
                }
            )
        )
        payment["% de viagens"] = (
            (payment["Total de viagens"] / payment["Total de viagens"].sum()) * 100
        ).round(2)

        st.bar_chart(payment["Total de viagens"], use_container_width=True)
        st.dataframe(payment, use_container_width=True)

    with tab_weekday:
        weekday = (
            filtered_df.groupby("day_of_week", as_index=True)
            .agg(
                total_trips=("day_of_week", "size"),
                avg_distance=("trip_distance", "mean"),
                avg_fare=("fare_amount", "mean"),
                avg_duration=("trip_duration_min", "mean"),
                avg_tip_pct=("tip_pct", "mean"),
            )
            .round(2)
            .rename(
                columns={
                    "total_trips": "Total de viagens",
                    "avg_distance": "Distancia media (mi)",
                    "avg_fare": "Tarifa media (US$)",
                    "avg_duration": "Duracao media (min)",
                    "avg_tip_pct": "Gorjeta media (%)",
                }
            )
        )
        weekday = weekday.reindex(DAY_ORDER)

        st.bar_chart(weekday["Total de viagens"], use_container_width=True)
        st.dataframe(weekday, use_container_width=True)


if __name__ == "__main__":
    main()
