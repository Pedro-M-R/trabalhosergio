import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import os
import re

# ─── CONFIG ───────────────────────────────────────────────────────────────────
CSV_DIR = "baixados_sia"
DATASETS = {
    "📦 Quantidade Aprovada": "SIA_Qtd.aprovada_certo.csv",
    "💰 Valor Aprovado":      "SIA_Valor_aprovado_certo.csv",
}

st.set_page_config(
    page_title="SIA Analytics",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🏥"
)

# ─── ESTILO ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
html, body, [class*="css"] { font-family: 'Sora', sans-serif; }
.stApp { background: #0f1117; color: #e8eaf0; }
section[data-testid="stSidebar"] { background: #161b27 !important; border-right: 1px solid #2a3045; }
section[data-testid="stSidebar"] * { color: #c9cfe0 !important; }

.kpi-card {
    background: #1a2035; border: 1px solid #2a3550; border-radius: 16px;
    padding: 1.4rem 1.6rem; position: relative; overflow: hidden;
    transition: transform .2s, box-shadow .2s;
}
.kpi-card:hover { transform: translateY(-3px); box-shadow: 0 8px 32px rgba(0,0,0,.4); }
.kpi-card::before { content:''; position:absolute; top:0; left:0; right:0; height:3px; }
.kpi-card.blue::before  { background: linear-gradient(90deg,#4f8ef7,#7b61ff); }
.kpi-card.green::before { background: linear-gradient(90deg,#22c55e,#16a34a); }
.kpi-card.amber::before { background: linear-gradient(90deg,#f59e0b,#f97316); }
.kpi-card.rose::before  { background: linear-gradient(90deg,#f43f5e,#ec4899); }
.kpi-label { font-size:.72rem; font-weight:600; letter-spacing:.1em; text-transform:uppercase; color:#6b7a99; margin-bottom:.35rem; }
.kpi-value { font-size:2rem; font-weight:700; color:#e8eaf0; line-height:1; }
.kpi-sub   { font-size:.78rem; color:#8896b3; margin-top:.4rem; }

.section-title { font-size:1.1rem; font-weight:700; color:#c4ccdf; border-left:3px solid #4f8ef7; padding-left:.75rem; margin:1.8rem 0 .3rem; }
.section-sub   { font-size:.82rem; color:#6b7a99; margin-bottom:1rem; padding-left:1.1rem; }

.insight { background:#1a2035; border:1px solid #2a3550; border-radius:12px; padding:1rem 1.2rem; margin-top:.8rem; font-size:.85rem; color:#a0aec0; line-height:1.6; }
.insight strong { color:#e8eaf0; }

.ranking-table { width:100%; border-collapse:collapse; }
.ranking-table th { font-size:.7rem; letter-spacing:.08em; text-transform:uppercase; color:#4f6080; padding:.5rem .8rem; border-bottom:1px solid #2a3045; text-align:left; }
.ranking-table td { padding:.6rem .8rem; border-bottom:1px solid #1e2840; font-size:.88rem; color:#c9cfe0; }
.ranking-table tr:hover td { background:#1e2840; }
.badge { display:inline-block; padding:.15rem .55rem; border-radius:99px; font-size:.72rem; font-weight:600; font-family:'JetBrains Mono',monospace; }
.badge-up   { background:#14532d; color:#4ade80; }
.badge-down { background:#4c0519; color:#fb7185; }
</style>
""", unsafe_allow_html=True)

# ─── HELPER: FORMATAÇÃO ROBUSTA ──────────────────────────────────────────────
def fmt(v):
    if isinstance(v, pd.Series):
        v = v.iloc[0] if len(v) > 0 else np.nan
    elif isinstance(v, (list, np.ndarray)):
        v = v[0] if len(v) > 0 else np.nan
    if pd.isna(v):
        return "—"
    try:
        v = float(v)
    except (TypeError, ValueError):
        return str(v)
    if abs(v) >= 1e9:
        return f"{v/1e9:.2f}B"
    if abs(v) >= 1e6:
        return f"{v/1e6:.1f}M"
    if abs(v) >= 1e3:
        return f"{v/1e3:.0f}k"
    return f"{v:,.0f}"

PLOT_CFG = dict(
    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
    font=dict(family='Sora', color='#8896b3', size=12),
    margin=dict(l=10, r=10, t=40, b=10),
    xaxis=dict(gridcolor='#1e2840', zerolinecolor='#2a3045', tickfont=dict(size=11)),
    yaxis=dict(gridcolor='#1e2840', zerolinecolor='#2a3045', tickfont=dict(size=11)),
)

def L(fig, title="", height=420):
    fig.update_layout(**PLOT_CFG, title=dict(text=title, font=dict(size=14, color='#c4ccdf'), x=0), height=height)
    return fig

# ─── CARREGAMENTO ─────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_dataset(filename: str) -> pd.DataFrame:
    path = f"{CSV_DIR}/{filename}"
    if not os.path.exists(path):
        st.error(f"❌ Arquivo não encontrado: {path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, sep=",", encoding="utf-8-sig", dtype=str, low_memory=False)
        df.columns = [str(c).strip().strip('"').strip("'").strip() for c in df.columns]
        df = df.loc[:, ~df.columns.str.startswith('Unnamed')]

        mun_col = None
        for c in df.columns:
            if 'munic' in c.lower() or c.lower() in ['municipio','município','cidade']:
                mun_col = c
                break
        if mun_col is None:
            mun_col = df.columns[0]

        df = df.set_index(mun_col)
        df.index = [str(i).strip().strip('"').strip("'") for i in df.index]
        df = df[df.index.notna() & (df.index != '') & (df.index != 'nan')]

        for col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str)
                    .str.replace('.', '', regex=False)
                    .str.replace(',', '.', regex=False)
                    .str.replace(r'[^\d.-]', '', regex=True),
                errors='coerce'
            )
        df = df.dropna(how='all').dropna(axis=1, how='all')
        return df
    except Exception as e:
        st.error(f"❌ Erro ao carregar: {e}")
        return pd.DataFrame()

# ─── SIDEBAR ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🏥 SIA Analytics")
    st.markdown("---")
    dataset_name = st.selectbox("Dataset", list(DATASETS.keys()))
    st.markdown("---")
    busca = st.text_input("🔍 Buscar município", placeholder="ex: São Paulo")
    st.markdown("---")
    st.markdown("**Seções**")
    show_overview   = st.checkbox("📊 Visão Geral", value=True)
    show_ranking    = st.checkbox("🏆 Ranking por Procedimento", value=True)
    show_munic      = st.checkbox("🏙️ Perfil de Município", value=True)
    show_compare    = st.checkbox("🔀 Comparar Municípios", value=True)
    show_timeseries = st.checkbox("📈 Evolução Temporal (Série)", value=True)
    show_dist       = st.checkbox("📦 Distribuição", value=False)
    show_raw        = st.checkbox("🗂️ Dados Brutos", value=False)

# ─── CARREGAR ─────────────────────────────────────────────────────────────────
with st.spinner("Carregando dados..."):
    df_full = load_dataset(DATASETS[dataset_name])

if df_full.empty:
    st.stop()

df = df_full.copy()

with st.sidebar:
    st.markdown("---")
    st.caption(f"✅ {len(df):,} municípios · {len(df.columns)} procedimentos")

if busca:
    mask = df.index.str.contains(busca, case=False, na=False)
    df = df.loc[mask]
    if df.empty:
        st.warning(f"Nenhum município encontrado para **{busca}**")
        st.stop()

proc_cols = list(df.columns)
if not proc_cols:
    st.error("❌ Nenhuma coluna de procedimento encontrada.")
    st.stop()

is_valor  = "Valor" in dataset_name
unidade   = "R$" if is_valor else "procedimentos"

def short_name(col, max_len=35):
    s = str(col).strip()
    return s if len(s) <= max_len else s[:max_len] + "…"

# ─── HEADER ───────────────────────────────────────────────────────────────────
st.markdown(f"""
<h1 style='font-size:1.8rem;font-weight:700;color:#e8eaf0;margin-bottom:.2rem'>🏥 Painel SIA</h1>
<p style='color:#6b7a99;font-size:.9rem;margin-bottom:1.5rem'>
    {dataset_name} &nbsp;·&nbsp; {len(df):,} municípios &nbsp;·&nbsp; {len(proc_cols)} grupos de procedimentos
</p>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# VISÃO GERAL (mantida)
# ═══════════════════════════════════════════════════════════════════════════════
if show_overview:
    st.markdown('<div class="section-title">📊 Visão Geral</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Totais consolidados de todos os municípios e procedimentos</div>', unsafe_allow_html=True)

    total_geral  = df[proc_cols].sum().sum()
    maior_proc   = df[proc_cols].sum().idxmax()
    mun_maior    = df[proc_cols].sum(axis=1).idxmax()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""<div class="kpi-card blue"><div class="kpi-label">Total geral</div><div class="kpi-value">{fmt(total_geral)}</div><div class="kpi-sub">{unidade}</div></div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""<div class="kpi-card green"><div class="kpi-label">Municípios</div><div class="kpi-value">{len(df):,}</div><div class="kpi-sub">{len(proc_cols)} procedimentos</div></div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""<div class="kpi-card amber"><div class="kpi-label">Principal procedimento</div><div class="kpi-value" style="font-size:1rem">{short_name(maior_proc, 28)}</div><div class="kpi-sub">{fmt(df[maior_proc].sum())} {unidade}</div></div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""<div class="kpi-card rose"><div class="kpi-label">Município líder</div><div class="kpi-value" style="font-size:1.1rem">{short_name(mun_maior, 22)}</div><div class="kpi-sub">{fmt(df.loc[mun_maior, proc_cols].sum())} {unidade}</div></div>""", unsafe_allow_html=True)

    # Barras horizontais
    totais_proc = df[proc_cols].sum().sort_values(ascending=False)
    fig_proc = go.Figure(go.Bar(
        x=totais_proc.values, y=[short_name(c,45) for c in totais_proc.index], orientation='h',
        marker=dict(color=totais_proc.values, colorscale=[[0,'#1e3a5f'],[0.5,'#3b82f6'],[1,'#7dd3fc']], showscale=False),
        text=[fmt(v) for v in totais_proc.values], textposition='outside'
    ))
    L(fig_proc, f"Total por procedimento ({unidade})", height=max(400, len(proc_cols)*30))
    fig_proc.update_layout(yaxis=dict(autorange='reversed'))
    st.plotly_chart(fig_proc, use_container_width=True)

    # Pizza
    top8 = totais_proc.head(8)
    outros = totais_proc.iloc[8:].sum()
    pie_data = pd.concat([top8, pd.Series({'Outros': outros})]) if outros>0 else top8
    fig_pie = px.pie(values=pie_data.values, names=[short_name(n,40) for n in pie_data.index], hole=0.45, color_discrete_sequence=px.colors.qualitative.Vivid)
    fig_pie.update_traces(textposition='inside', textinfo='percent+label')
    L(fig_pie, "Participação %", 420)
    st.plotly_chart(fig_pie, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# RANKING POR PROCEDIMENTO
# ═══════════════════════════════════════════════════════════════════════════════
if show_ranking:
    st.markdown('<div class="section-title">🏆 Ranking por Procedimento</div>', unsafe_allow_html=True)
    proc_sel = st.selectbox("Procedimento:", proc_cols, format_func=lambda c: short_name(c,70), key="rank_proc")
    n_rank = st.slider("Quantos municípios:", 5, 30, 15, key="rank_n")
    top = df[proc_sel].dropna().nlargest(n_rank)
    fig_bar = go.Figure(go.Bar(
        x=top.values, y=top.index, orientation='h',
        marker=dict(color=top.values, colorscale=[[0,'#1e3a5f'],[0.5,'#3b82f6'],[1,'#7dd3fc']], showscale=False),
        text=[fmt(v) for v in top.values], textposition='outside'
    ))
    L(fig_bar, f"Top {n_rank} — {short_name(proc_sel,50)}", height=max(300, n_rank*34))
    fig_bar.update_layout(yaxis=dict(autorange='reversed'))
    st.plotly_chart(fig_bar, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PERFIL DE MUNICÍPIO
# ═══════════════════════════════════════════════════════════════════════════════
if show_munic:
    st.markdown('<div class="section-title">🏙️ Perfil de Município</div>', unsafe_allow_html=True)
    mun_sel = st.selectbox("Município:", df.index.tolist(), key="munic_sel")
    if mun_sel:
        row_series = df.loc[mun_sel, proc_cols]
        if isinstance(row_series, pd.DataFrame):
            row_series = row_series.iloc[0]
        row = row_series.dropna().sort_values(ascending=False)
        total_mun = row.sum()
        if total_mun > 0:
            rank_nac = int((df[proc_cols].sum(axis=1) > total_mun).sum()) + 1
        else:
            rank_nac = len(df)
        c1,c2,c3 = st.columns(3)
        with c1: st.metric("Total do município", fmt(total_mun), help=unidade)
        with c2: st.metric("Ranking nacional", f"#{rank_nac}", help=f"de {len(df)}")
        with c3: st.metric("Principal procedimento", short_name(row.index[0],30), help=f"{row.iloc[0]/total_mun*100:.1f}% do total")
        fig_mun = go.Figure(go.Bar(
            x=row.values, y=[short_name(c,45) for c in row.index], orientation='h',
            marker=dict(color=row.values, colorscale=[[0,'#312e81'],[0.5,'#7c3aed'],[1,'#c4b5fd']], showscale=False),
            text=[fmt(v) for v in row.values], textposition='outside'
        ))
        L(fig_mun, f"Procedimentos — {mun_sel}", height=max(350, len(row)*28))
        fig_mun.update_layout(yaxis=dict(autorange='reversed'))
        st.plotly_chart(fig_mun, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# COMPARAR MUNICÍPIOS
# ═══════════════════════════════════════════════════════════════════════════════
if show_compare:
    st.markdown('<div class="section-title">🔀 Comparar Municípios</div>', unsafe_allow_html=True)
    default_cmp = df[proc_cols].sum(axis=1).nlargest(3).index.tolist()
    munis_cmp = st.multiselect("Selecione municípios (2 a 6):", df.index.tolist(), default=default_cmp, key="cmp_munis")
    if len(munis_cmp) >= 2:
        df_cmp = df.loc[munis_cmp, proc_cols].T
        df_cmp.index = [short_name(c,45) for c in df_cmp.index]
        top_procs = df[proc_cols].sum().nlargest(10).index.tolist()
        df_radar = df.loc[munis_cmp, top_procs]
        fig_radar = go.Figure()
        colors = px.colors.qualitative.Vivid
        for i, mun in enumerate(munis_cmp):
            vals = df_radar.loc[mun]
            if isinstance(vals, pd.DataFrame):
                vals = vals.iloc[0]
            vals = vals.fillna(0).tolist()
            vals += [vals[0]]
            cats = [short_name(c,30) for c in top_procs] + [short_name(top_procs[0],30)]
            fig_radar.add_trace(go.Scatterpolar(r=vals, theta=cats, fill='toself', name=mun,
                line=dict(color=colors[i%len(colors)], width=2),
                fillcolor=colors[i%len(colors)].replace('rgb','rgba').replace(')',',0.08)')))
        L(fig_radar, "Perfil comparativo (top 10 procedimentos)", 500)
        fig_radar.update_layout(polar=dict(bgcolor='#1a2035', radialaxis=dict(gridcolor='#2a3550'), angularaxis=dict(gridcolor='#2a3550')))
        st.plotly_chart(fig_radar, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# DISTRIBUIÇÃO (Boxplot simplificado)
# ═══════════════════════════════════════════════════════════════════════════════
if show_dist:
    st.markdown('<div class="section-title">📦 Distribuição dos Valores</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Distribuição dos municípios por procedimento (boxplot)</div>', unsafe_allow_html=True)
    
    procs_dist = st.multiselect(
        "Selecione os procedimentos (máx. 8 recomendado):",
        proc_cols,
        default=proc_cols[:min(5, len(proc_cols))],
        format_func=lambda c: short_name(c, 60),
        key="dist_procs"
    )
    
    if procs_dist:
        log_scale = st.checkbox("Escala logarítmica (recomendado para dados assimétricos)", value=True, key="dist_log")
        
        # Amostrar se muitos dados
        if len(df) > 5000:
            df_sample = df[procs_dist].dropna(how='all').sample(5000, random_state=42)
        else:
            df_sample = df[procs_dist].dropna(how='all')
        
        # Preparar dados longos
        df_reset = df_sample.reset_index()
        idx_col = df_reset.columns[0]  # coluna do município
        df_reset = df_reset.rename(columns={idx_col: 'Município'})
        df_long = df_reset.melt(id_vars='Município', var_name='Procedimento', value_name='Valor').dropna()
        df_long['Procedimento_short'] = df_long['Procedimento'].apply(lambda c: short_name(c, 40))
        
        # Boxplot
        fig_box = px.box(
            df_long,
            x='Procedimento_short',
            y='Valor',
            color='Procedimento_short',
            color_discrete_sequence=px.colors.qualitative.Vivid,
            points=False,  # sem outliers para manter limpo
            log_y=log_scale,
            title=f"Distribuição por procedimento ({unidade})"
        )
        L(fig_box, "", 500)
        fig_box.update_layout(showlegend=False, xaxis=dict(tickangle=-35))
        st.plotly_chart(fig_box, use_container_width=True)
        
        # Tabela de estatísticas
        st.subheader("📊 Estatísticas descritivas")
        stats_list = []
        for proc in procs_dist:
            s = df[proc].dropna()
            stats_list.append({
                'Procedimento': short_name(proc, 50),
                'Total': fmt(s.sum()),
                'Média': fmt(s.mean()),
                'Mediana': fmt(s.median()),
                'Desvio Padrão': fmt(s.std()),
                'Q1 (25%)': fmt(s.quantile(0.25)),
                'Q3 (75%)': fmt(s.quantile(0.75))
            })
        st.dataframe(pd.DataFrame(stats_list), use_container_width=True, hide_index=True)
        
        st.markdown("""
        <div class="insight">
            💡 <strong>Interpretação do boxplot:</strong><br>
            • A caixa representa 50% dos municípios (do 1º ao 3º quartil).<br>
            • A linha dentro da caixa é a <strong>mediana</strong>.<br>
            • Os "bigodes" indicam a faixa normal (1.5× IQR).<br>
            • Pontos acima dos bigodes são <strong>outliers</strong> (municípios com valores muito altos).
        </div>
        """, unsafe_allow_html=True)
    else:
        st.info("👆 Selecione pelo menos um procedimento.")

# ═══════════════════════════════════════════════════════════════════════════════
# DADOS BRUTOS (mantido)
# ═══════════════════════════════════════════════════════════════════════════════
if show_raw:
    st.markdown('<div class="section-title">🗂️ Dados Brutos</div>', unsafe_allow_html=True)
    procs_raw = st.multiselect("Procedimentos:", proc_cols, default=proc_cols[:6], format_func=lambda c: short_name(c,60), key="raw_procs")
    n_linhas = st.slider("Linhas:", 10, 200, 30)
    if procs_raw:
        st.dataframe(df[procs_raw].head(n_linhas).style.format(lambda v: fmt(v) if pd.notna(v) else "—").background_gradient(cmap='Blues'), use_container_width=True)
        csv = df[procs_raw].to_csv(index=True, encoding='utf-8-sig')
        st.download_button("📥 Baixar CSV", csv, f"SIA_{dataset_name.replace(' ','_')}.csv", "text/csv")

st.markdown("""
<div style='margin-top:3rem;padding-top:1.5rem;border-top:1px solid #1e2840;text-align:center;color:#3d4f6e;font-size:.78rem'>
    SIA Analytics · DATASUS
</div>
""", unsafe_allow_html=True)