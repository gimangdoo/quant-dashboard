import streamlit as st
import pandas as pd
import numpy as np
import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import FinanceDataReader as fdr
import gspread
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold

# ⚙️ 페이지 설정
st.set_page_config(page_title="Quant Vertical Screener", layout="wide", initial_sidebar_state="expanded")

# 🚨 [사용자 입력 필수] 본인의 구글 시트 URL 3개
URL_MASTER_DB = 'https://docs.google.com/spreadsheets/d/1a1--xht5ahEVtZH8SjpOakwHVoMSMJtIw9T1HTanw1M/edit?gid=0#gid=0'
URL_SOURCE_KOSPI = 'https://docs.google.com/spreadsheets/d/1GHB9J_hN13cBSXVCeilBuXFDoBSQMIboxSBzZVtr7KE/edit?gid=0#gid=0'
URL_SOURCE_KOSDAQ = 'https://docs.google.com/spreadsheets/d/1gOwo4Z_vQhdCab3XZ0RBtOzRUdR1XGg3NhAtAN18Ikg/edit?gid=0#gid=0'

@st.cache_data(ttl=3600)
def load_data():
    credentials_dict = dict(st.secrets["gcp_service_account"])
    gc = gspread.service_account_from_dict(credentials_dict)
    
    def get_df(url):
        sheet = gc.open_by_url(url).sheet1
        data = sheet.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        code_col = next((c for c in df.columns if '코드' in c or 'Code' in c or '종목' in c or 'ticker' in c.lower()), None)
        if code_col:
            df['종목코드'] = df[code_col].astype(str).str.extract(r'(\d+)')[0].str.zfill(6)
        return df

    master_df = get_df(URL_MASTER_DB)
    kospi_df = get_df(URL_SOURCE_KOSPI)
    kosdaq_df = get_df(URL_SOURCE_KOSDAQ)
    
    master_df = master_df[master_df['데이터_상태'].str.contains('✅ 정상', na=False)].copy()
        
    time_cols = [c for c in master_df.columns if '_' in c]
    for c in time_cols:
        master_df[c] = pd.to_numeric(master_df[c].astype(str).str.replace(',', ''), errors='coerce')
        
    def find_rs_col(df):
        return next((c for c in df.columns if 'rs' in c.lower()), None)
        
    rs_kpi, rs_kdq = find_rs_col(kospi_df), find_rs_col(kosdaq_df)
    
    kpi_merged = pd.merge(kospi_df[['종목코드', rs_kpi]].rename(columns={rs_kpi:'RS'}), master_df, on='종목코드', how='inner')
    kdq_merged = pd.merge(kosdaq_df[['종목코드', rs_kdq]].rename(columns={rs_kdq:'RS'}), master_df, on='종목코드', how='inner')
    
    try:
        krx_df = fdr.StockListing('KRX')
        code_col_krx = 'Symbol' if 'Symbol' in krx_df.columns else 'Code'
        krx_dict = dict(zip(krx_df[code_col_krx].astype(str).str.zfill(6), krx_df['Name']))
        kpi_merged['종목명'] = kpi_merged['종목코드'].map(krx_dict).fillna(kpi_merged['종목코드'])
        kdq_merged['종목명'] = kdq_merged['종목코드'].map(krx_dict).fillna(kdq_merged['종목코드'])
    except:
        try:
            backup_df = pd.read_csv("https://raw.githubusercontent.com/corazzon/finance-data-analysis/main/krx.csv")
            code_col_bk = 'Symbol' if 'Symbol' in backup_df.columns else 'Code'
            krx_dict = dict(zip(backup_df[code_col_bk].astype(str).str.zfill(6), backup_df['Name']))
            kpi_merged['종목명'] = kpi_merged['종목코드'].map(krx_dict).fillna(kpi_merged['종목코드'])
            kdq_merged['종목명'] = kdq_merged['종목코드'].map(krx_dict).fillna(kdq_merged['종목코드'])
        except:
            kpi_merged['종목명'] = kpi_merged['종목코드']
            kdq_merged['종목명'] = kdq_merged['종목코드']
    
    kpi_merged['RS'] = pd.to_numeric(kpi_merged['RS'], errors='coerce').fillna(0)
    kdq_merged['RS'] = pd.to_numeric(kdq_merged['RS'], errors='coerce').fillna(0)
    kpi_merged = kpi_merged.sort_values(by='RS', ascending=False).reset_index(drop=True)
    kdq_merged = kdq_merged.sort_values(by='RS', ascending=False).reset_index(drop=True)
    
    return kpi_merged, kdq_merged

def calculate_growth(row):
    cols = [c for c in row.index if type(c) == str and '_' in c]
    q_cols = sorted([c for c in cols if 'Q' in c], reverse=True)
    y_cols = sorted([c for c in cols if '1Y' in c], reverse=True)
    
    def get_yoy(latest_cols, required_pairs):
        growth_data = []
        for c in latest_cols:
            if len(growth_data) >= required_pairs: break
            try:
                y, q = c.split('_')
                prev_c = f"{int(y)-1}_{q}"
                if prev_c in cols and pd.notna(row.get(c)) and pd.notna(row.get(prev_c)):
                    curr_val, prev_val = row[c], row[prev_c]
                    if prev_val != 0:
                        g_rate = ((curr_val - prev_val) / abs(prev_val)) * 100
                        md = {'1Q':'03-31', '2Q':'06-30', '3Q':'09-30', '4Q':'12-31', '1Y':'12-31'}.get(q, '12-31')
                        dt = pd.to_datetime(f"{y}-{md}")
                        growth_data.append({'Date': dt, 'Period': c, 'Growth': g_rate})
            except: pass
        return pd.DataFrame(growth_data)

    q_growth = get_yoy(q_cols, 4)
    y_growth = get_yoy(y_cols, 3)
    return q_growth, y_growth

def draw_index_chart(market_name, view_mode):
    sym = 'KS11' if "KOSPI" in market_name else 'KQ11'
    name = "KOSPI 벤치마크 지수" if "KOSPI" in market_name else "KOSDAQ 벤치마크 지수"
    
    end_date = datetime.date.today()
    if view_mode == "📱 모바일 모드 (최근 1년 줌인)":
        start_view = end_date - datetime.timedelta(days=365)
    else:
        start_view = end_date - datetime.timedelta(days=1095) 
        
    start_fetch = start_view - datetime.timedelta(days=300) 
    
    try:
        df_price = fdr.DataReader(sym, start_fetch, end_date)
        if df_price.empty: return None
    except: return None

    df_price['Change_Pct'] = df_price['Close'].pct_change() * 100
    df_price['Vol_Change_Pct'] = df_price['Volume'].pct_change() * 100
    
    df_price['SMA50'] = df_price['Close'].rolling(window=50).mean()
    df_price['SMA150'] = df_price['Close'].rolling(window=150).mean()
    df_price['SMA200'] = df_price['Close'].rolling(window=200).mean()
    
    df_view = df_price[df_price.index >= pd.to_datetime(start_view)].reset_index()
    df_view['idx'] = np.arange(len(df_view))
    
    df_view['Change_Pct'] = df_view['Change_Pct'].fillna(0)
    df_view['Vol_Change_Pct'] = df_view['Vol_Change_Pct'].fillna(0)
    
    df_view['YearMonth'] = df_view['Date'].dt.strftime('%Y-%m')
    first_days = df_view.drop_duplicates(subset=['YearMonth'], keep='first')
    tickvals = first_days['idx'].tolist()
    ticktext = [f"{d.year}년 {d.month}월" if d.month == 1 else f"{d.month}월" for d in first_days['Date']]

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, 
        row_heights=[0.8, 0.2], vertical_spacing=0.03,
        specs=[[{"secondary_y": False}], [{"secondary_y": False}]] 
    )
    
    custom_data = np.stack((
        df_view['Date'].dt.strftime('%Y-%m-%d'),
        df_view['Change_Pct'],
        df_view['Volume'],
        df_view['Vol_Change_Pct']
    ), axis=-1)

    fig.add_trace(go.Candlestick(
        x=df_view['idx'], open=df_view['Open'], high=df_view['High'], 
        low=df_view['Low'], close=df_view['Close'], name='일봉',
        increasing_line_color='#FF4136', increasing_line_width=1, increasing_fillcolor='#FF4136',
        decreasing_line_color='#0074D9', decreasing_line_width=1, decreasing_fillcolor='#0074D9',
        opacity=1.0, showlegend=False, customdata=custom_data,
        hovertemplate="날짜: %{customdata[0]}<br>시가: %{open:,.2f}<br>고가: %{high:,.2f}<br>저가: %{low:,.2f}<br>종가: %{close:,.2f}<br>변동률: %{customdata[1]:.2f}%<br>거래량: %{customdata[2]:,}<br>전일대비거래량: %{customdata[3]:.2f}%<extra></extra>"
    ), row=1, col=1)

    for sma, color in zip(['SMA50', 'SMA150', 'SMA200'], ['orange', 'purple', 'gray']):
        fig.add_trace(go.Scatter(
            x=df_view['idx'], y=df_view[sma], name=sma, 
            line=dict(color=color, width=1.0), hoverinfo='skip'
        ), row=1, col=1)

    vol_colors = ['#FF4136' if c >= o else '#0074D9' for c, o in zip(df_view['Close'], df_view['Open'])]
    fig.add_trace(go.Bar(
        x=df_view['idx'], y=df_view['Volume'], 
        marker_color=vol_colors, opacity=0.8, name='거래량', hoverinfo='skip'
    ), row=2, col=1)

    max_price = df_view['High'].max()
    max_vol = df_view['Volume'].max()
    
    fig.update_xaxes(showticklabels=False, showgrid=False, zeroline=False, row=1, col=1)
    fig.update_xaxes(
        tickmode='array', tickvals=tickvals, ticktext=ticktext, showticklabels=True, 
        showgrid=False, zeroline=False, row=2, col=1
    )
    
    # 🎯 [핵심 패치] 지수 차트의 Y축 고정(fixedrange) 잠금 해제 (False)
    fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='#F0F0F0', zeroline=False, row=1, col=1)
    fig.update_yaxes(showgrid=False, zeroline=False, row=2, col=1)
    
    fig.update_layout(
        title=dict(text=f"<b>📊 {name}</b>", font=dict(size=20, color='#2c3e50'), x=0.02),
        xaxis=dict(rangeslider=dict(visible=False)),
        yaxis=dict(title="지수", side="right", fixedrange=False, range=[df_view['Low'].min() * 0.9, max_price * (8/7)]),
        yaxis2=dict(fixedrange=False, range=[0, max_vol * (8/7)]), 
        plot_bgcolor='#FAFAFA', paper_bgcolor='#FAFAFA', hovermode='x',
        legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="right", x=1, font=dict(size=10)),
        margin=dict(l=40, r=40, t=60, b=20), height=450
    )
    
    return fig

def draw_stock_chart(row, view_mode):
    sym, name, rs = row['종목코드'], row.get('종목명', ''), row.get('RS', 0)
    
    end_date = datetime.date.today()
    if view_mode == "📱 모바일 모드 (최근 1년 줌인)":
        start_view = end_date - datetime.timedelta(days=365)
    else:
        start_view = end_date - datetime.timedelta(days=1095) 
        
    start_fetch = start_view - datetime.timedelta(days=300) 
    
    try:
        df_price = fdr.DataReader(sym, start_fetch, end_date)
        if df_price.empty: return go.Figure().update_layout(title="주가 데이터 없음")
    except: return go.Figure().update_layout(title="주가 API 로드 실패")

    df_price['Change_Pct'] = df_price['Close'].pct_change() * 100
    df_price['Vol_Change_Pct'] = df_price['Volume'].pct_change() * 100
    
    df_price['SMA50'] = df_price['Close'].rolling(window=50).mean()
    df_price['SMA150'] = df_price['Close'].rolling(window=150).mean()
    df_price['SMA200'] = df_price['Close'].rolling(window=200).mean()
    
    df_view = df_price[df_price.index >= pd.to_datetime(start_view)].reset_index()
    df_view['idx'] = np.arange(len(df_view))
    
    df_view['Change_Pct'] = df_view['Change_Pct'].fillna(0)
    df_view['Vol_Change_Pct'] = df_view['Vol_Change_Pct'].fillna(0)
    
    df_view['YearMonth'] = df_view['Date'].dt.strftime('%Y-%m')
    first_days = df_view.drop_duplicates(subset=['YearMonth'], keep='first')
    tickvals = first_days['idx'].tolist()
    ticktext = [f"{d.year}년 {d.month}월" if d.month == 1 else f"{d.month}월" for d in first_days['Date']]

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, 
        row_heights=[0.8, 0.2], vertical_spacing=0.03,
        specs=[[{"secondary_y": True}], [{"secondary_y": False}]]
    )
    
    custom_data = np.stack((
        df_view['Date'].dt.strftime('%Y-%m-%d'),
        df_view['Change_Pct'],
        df_view['Volume'],
        df_view['Vol_Change_Pct']
    ), axis=-1)

    fig.add_trace(go.Candlestick(
        x=df_view['idx'], open=df_view['Open'], high=df_view['High'], 
        low=df_view['Low'], close=df_view['Close'], name='일봉',
        increasing_line_color='#FF4136', increasing_line_width=1, increasing_fillcolor='#FF4136',
        decreasing_line_color='#0074D9', decreasing_line_width=1, decreasing_fillcolor='#0074D9',
        opacity=1.0, showlegend=False, customdata=custom_data,
        hovertemplate="날짜: %{customdata[0]}<br>시가: %{open:,.0f}원<br>고가: %{high:,.0f}원<br>저가: %{low:,.0f}원<br>종가: %{close:,.0f}원<br>변동률: %{customdata[1]:.2f}%<br>거래량: %{customdata[2]:,}주<br>전일대비거래량: %{customdata[3]:.2f}%<extra></extra>"
    ), row=1, col=1, secondary_y=True)

    for sma, color in zip(['SMA50', 'SMA150', 'SMA200'], ['orange', 'purple', 'gray']):
        fig.add_trace(go.Scatter(
            x=df_view['idx'], y=df_view[sma], name=sma, 
            line=dict(color=color, width=1.0), hoverinfo='skip'
        ), row=1, col=1, secondary_y=True)

    q_growth, y_growth = calculate_growth(row)
    growth_values = []
    df_view_sorted = df_view[['Date', 'idx']].sort_values('Date')

    if not q_growth.empty:
        q_growth = q_growth[q_growth['Date'] >= pd.to_datetime(start_view)].sort_values('Date')
        if not q_growth.empty:
            q_growth = pd.merge_asof(q_growth, df_view_sorted, on='Date', direction='nearest')
            fig.add_trace(go.Scatter(
                x=q_growth['idx'], y=q_growth['Growth'], text=q_growth['Period'], name='분기 증감률',
                mode='lines+markers', line=dict(color='#A9A9A9', width=1.5, dash='dot'), marker=dict(size=4, symbol='circle'),
                customdata=q_growth['Date'].dt.strftime('%Y-%m-%d'),
                hovertemplate="결산일: %{customdata}<br>기간: %{text}<br>증감률: %{y:.2f}%<extra></extra>"
            ), row=1, col=1, secondary_y=False)
            growth_values.extend(q_growth['Growth'].tolist())

    if not y_growth.empty:
        y_growth = y_growth[y_growth['Date'] >= pd.to_datetime(start_view)].sort_values('Date')
        if not y_growth.empty:
            y_growth = pd.merge_asof(y_growth, df_view_sorted, on='Date', direction='nearest')
            fig.add_trace(go.Scatter(
                x=y_growth['idx'], y=y_growth['Growth'], text=y_growth['Period'], name='연간 증감률',
                mode='lines+markers', line=dict(color='#555555', width=1.5), marker=dict(size=4, symbol='circle'),
                customdata=y_growth['Date'].dt.strftime('%Y-%m-%d'),
                hovertemplate="결산일: %{customdata}<br>기간: %{text}<br>증감률: %{y:.2f}%<extra></extra>"
            ), row=1, col=1, secondary_y=False)
            growth_values.extend(y_growth['Growth'].tolist())

    if growth_values:
        g_min, g_max = min(growth_values), max(growth_values)
        g_range = g_max - g_min if g_max != g_min else 100
        y_left_min, y_left_max = g_min - (g_range * 0.1), g_max + (g_range * 0.1)
    else:
        y_left_min, y_left_max = -100, 100

    vol_colors = ['#FF4136' if c >= o else '#0074D9' for c, o in zip(df_view['Close'], df_view['Open'])]
    fig.add_trace(go.Bar(
        x=df_view['idx'], y=df_view['Volume'], 
        marker_color=vol_colors, opacity=0.8, name='거래량', hoverinfo='skip'
    ), row=2, col=1)

    max_price = df_view['High'].max()
    max_vol = df_view['Volume'].max()
    
    fig.update_xaxes(showticklabels=False, showgrid=False, zeroline=False, row=1, col=1)
    fig.update_xaxes(
        tickmode='array', tickvals=tickvals, ticktext=ticktext, showticklabels=True, 
        showgrid=False, zeroline=False, row=2, col=1
    )
    
    fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='#F0F0F0', zeroline=False, row=1, col=1)
    fig.update_yaxes(showgrid=False, zeroline=False, row=2, col=1)
    
    fig.update_layout(
        title=dict(text=f"<b>{name}</b> ({sym}) | RS: {rs:.1f}", font=dict(size=18, color='black'), x=0.02),
        xaxis=dict(rangeslider=dict(visible=False)),
        
        # 🎯 [핵심 패치 2] 개별 종목 차트의 모든 Y축 고정(fixedrange) 잠금 해제 (False)
        yaxis=dict(title="성장률 (%)", side="left", showgrid=False, fixedrange=False, range=[y_left_min, y_left_max]), 
        yaxis2=dict(title="주가 (원)", side="right", fixedrange=False, range=[df_view['Low'].min() * 0.9, max_price * (8/7)]),
        yaxis3=dict(fixedrange=False, range=[0, max_vol * (8/7)]), 
        
        plot_bgcolor='white', paper_bgcolor='white', hovermode='x',
        legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="right", x=1, font=dict(size=10)),
        margin=dict(l=40, r=40, t=60, b=20), height=450
    )
    return fig

# ==========================================
# 🚀 메인 대시보드 UI 및 렌더링
# ==========================================

if 'page_num' not in st.session_state:
    st.session_state.page_num = 1

st.sidebar.title("🧭 시장 선택")

def reset_page():
    st.session_state.page_num = 1

market = st.sidebar.radio("트렌드 템플릿 선택", ("KOSPI (코스피)", "KOSDAQ (코스닥)"), on_change=reset_page)

st.sidebar.markdown("---")
view_mode = st.sidebar.radio("🖥️ 화면 모드", ("💻 PC 모드 (최근 3년 파노라마)", "📱 모바일 모드 (최근 1년 줌인)"))

try:
    kpi_df, kdq_df = load_data()
    target_df = kpi_df if "KOSPI" in market else kdq_df
    
    if target_df.empty:
        st.warning("선택한 시장의 정상 종목 데이터가 없습니다. URL을 확인하세요.")
        st.stop()
        
    st.sidebar.markdown(f"**검출된 종목:** 총 {len(target_df)}개")
    
    st.markdown("<style> .stPlotlyChart {border-radius: 10px; box-shadow: 2px 2px 10px rgba(0,0,0,0.1); margin-bottom: 20px;} </style>", unsafe_allow_html=True)
    
    index_fig = draw_index_chart(market, view_mode)
    if index_fig:
        # 🎯 [핵심 패치 3] Streamlit 렌더링 엔진에 휠 줌(scrollZoom: True) 권한 부여
        st.plotly_chart(index_fig, use_container_width=True, config={'scrollZoom': True})
        st.markdown("<hr style='border: 1px solid #e0e0e0; margin: 30px 0;'>", unsafe_allow_html=True) 

    items_per_page = 4
    total_pages = (len(target_df) // items_per_page) + (1 if len(target_df) % items_per_page > 0 else 0)
    
    if st.session_state.page_num > total_pages:
        st.session_state.page_num = max(1, total_pages)
    
    start_idx = (st.session_state.page_num - 1) * items_per_page
    view_df = target_df.iloc[start_idx:start_idx + items_per_page]
    
    for _, row in view_df.iterrows():
        sym = row['종목코드']
        name = row.get('종목명', sym)
        rs = row.get('RS', 0)
        
        fig = draw_stock_chart(row, view_mode)
        
        # 🎯 [핵심 패치 4] 개별 차트 렌더링에도 휠 줌(scrollZoom: True) 권한 부여
        st.plotly_chart(fig, use_container_width=True, config={'scrollZoom': True})
        
        st.markdown(f"""
        <div style="text-align: right; margin-top: -25px; margin-bottom: 10px; padding-right: 40px;">
            <a href="https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{sym}" target="_blank" 
               style="text-decoration: none; font-size: 12px; color: #555; background-color: #f8f9fa; border: 1px solid #ddd; padding: 4px 10px; border-radius: 4px; margin-right: 8px; font-weight: bold;">
               📊 FnGuide
            </a>
            <a href="https://finance.naver.com/item/main.naver?code={sym}" target="_blank" 
               style="text-decoration: none; font-size: 12px; color: #555; background-color: #f8f9fa; border: 1px solid #ddd; padding: 4px 10px; border-radius: 4px; font-weight: bold;">
               📰 Naver
            </a>
        </div>
        """, unsafe_allow_html=True)
        
        col_empty, col_btn = st.columns([4, 1.5]) 
        with col_btn:
            if st.button(f"🧠 {name} 심층 분석 가동", key=f"ai_{sym}"):
                with st.spinner("수석 분석가가 실시간 웹 검색 및 최신 리포트를 수집 중입니다... (약 5~8초 소요)"):
                    try:
                        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                        
                        analyst_persona = """
                        당신의 역할은 전 영역을 아우르는 '수석 전략 분석가(Chief Strategy Analyst)'입니다. 
                        단순한 과거 정보 나열을 철저히 배제하고, '가장 최신의 실시간 데이터(최근 1주일 내 뉴스, 애널리스트 목표가, 어닝 리포트)'를 반드시 검색하여 반영해야 합니다.
                        아래의 [Deep Analysis Process] 3단계를 엄격히 지켜 마크다운 형식으로 답변하십시오.

                        (Step 1) 두괄식 핵심 요약 (Executive Summary)
                        - 결론과 '가장 최신 모멘텀'을 3줄 이내 요약(Bullet points)으로 최상단에 제시.

                        (Step 2) 입체적 분석 (Tree of Thoughts)
                        - (A) 주류 관점 (Thesis): 해당 기업의 본질적 BM 및 '최신 증권사 컨센서스/긍정적 뉴스'.
                        - (B) 비판적 관점/리스크 (Antithesis): 최근 불거진 악재, 매크로 리스크, 실적 우려 (필수 포함).
                        - (C) 통합적 통찰 (Synthesis): 위 두 관점을 종합한 단기/중장기 투자 결론.

                        (Step 3) 논리 전개 (Chain-of-Thought)
                        - 현재 주식 시장의 최신 매크로 환경과 주어진 퀀트 데이터(RS)를 결합하여 최종 논리 전개.
                        """
                        
                        model = genai.GenerativeModel(
                            model_name='gemini-1.5-pro',
                            system_instruction=analyst_persona,
                            tools='google_search_retrieval' 
                        )
                        
                        prompt = f"""
                        현재 스크리닝 시스템에 포착된 한국 주식은 '{name}' (종목코드: {sym})입니다.
                        상대강도(RS) 점수는 {rs:.1f}점입니다.
                        
                        [필수 수행 명령]
                        지금 즉시 인터넷을 검색하여 '{name}'에 대한 가장 최근의 증권사 애널리스트 리포트 동향, 이번 달의 주요 공시, 그리고 현재 주가를 움직이는 핵심 뉴스 테마를 찾아내십시오.
                        과거의 지식에 의존하지 말고, 방금 검색한 '최신 팩트'와 RS 점수({rs:.1f})를 결합하여 상위 1% 트레이더를 위한 심층 브리핑을 작성하십시오.
                        """
                        
                        safety_settings = {
                            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
                        }
                        
                        response = model.generate_content(prompt, safety_settings=safety_settings)
                        
                        st.markdown(f"""
                        <div style="background-color: #F8F9FA; padding: 20px; border-radius: 8px; border-left: 5px solid #2C3E50; margin-bottom: 30px;">
                            <h4 style="color: #2C3E50; margin-top: 0;">🧠 실시간 데이터 기반 수석 분석가 브리핑</h4>
                            {response.text}
                        </div>
                        """, unsafe_allow_html=True)
                        
                    except Exception as e:
                        st.error(f"API 호출 실패 (키 설정 또는 네트워크 문제): {e}")
        
        st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.number_input(
            f"📄 페이지 이동 (1 ~ {total_pages})", 
            min_value=1, max_value=total_pages, 
            key='page_num'
        )

except Exception as e:
    st.error("🚨 앗! 데이터 연결 또는 렌더링 중 문제가 발생했습니다.")
    st.exception(e)
