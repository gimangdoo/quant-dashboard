import streamlit as st
import pandas as pd
import numpy as np
import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import FinanceDataReader as fdr
import gspread

# ⚙️ 페이지 설정 (전체화면, 사이드바 기본 숨김 지원)
st.set_page_config(page_title="Quant Trend Screener", layout="wide", initial_sidebar_state="expanded")

# 🚨 [사용자 입력 필수] 본인의 구글 시트 URL 3개를 정확히 입력하세요!
URL_MASTER_DB = '여기에_마스터DB_주소를_입력하세요'
URL_SOURCE_KOSPI = '여기에_코스피_트렌드템플릿_주소를_입력하세요'
URL_SOURCE_KOSDAQ = '여기에_코스닥_트렌드템플릿_주소를_입력하세요'

@st.cache_data(ttl=3600)
def load_data():
    """ 클라우드 전용 인증(Secrets)을 사용한 3개 시트 조인 및 퀀트 유니버스 생성 """
    
    # 🚨 코랩 인증 제거 -> Streamlit Secrets 기반 인증
    credentials_dict = dict(st.secrets["gcp_service_account"])
    gc = gspread.service_account_from_dict(credentials_dict)
    
    def get_df(url):
        sheet = gc.open_by_url(url).sheet1
        data = sheet.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        # 종목코드 규격화 (6자리 문자열)
        code_col = next((c for c in df.columns if '코드' in c or 'Code' in c or '종목' in c), None)
        if code_col:
            df['종목코드'] = df[code_col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.zfill(6)
        return df

    master_df = get_df(URL_MASTER_DB)
    kospi_df = get_df(URL_SOURCE_KOSPI)
    kosdaq_df = get_df(URL_SOURCE_KOSDAQ)
    
    # 마스터 DB 정상 종목만 숫자형으로 변환
    master_df = master_df[master_df['데이터_상태'].str.contains('✅ 정상', na=False)].copy()
    time_cols = [c for c in master_df.columns if '_' in c]
    for c in time_cols:
        master_df[c] = pd.to_numeric(master_df[c].astype(str).str.replace(',', ''), errors='coerce')
        
    # RS 열 찾기 (트렌드 템플릿에서 'RS' 또는 'rs'가 포함된 열)
    def find_rs_col(df):
        return next((c for c in df.columns if 'rs' in c.lower()), None)
        
    rs_kpi, rs_kdq = find_rs_col(kospi_df), find_rs_col(kosdaq_df)
    
    # 조인 (마스터 DB + RS 값)
    kpi_merged = pd.merge(kospi_df[['종목코드', rs_kpi]].rename(columns={rs_kpi:'RS'}), master_df, on='종목코드', how='inner')
    kdq_merged = pd.merge(kosdaq_df[['종목코드', rs_kdq]].rename(columns={rs_kdq:'RS'}), master_df, on='종목코드', how='inner')
    
    # RS 기준으로 내림차순 정렬
    kpi_merged['RS'] = pd.to_numeric(kpi_merged['RS'], errors='coerce').fillna(0)
    kdq_merged['RS'] = pd.to_numeric(kdq_merged['RS'], errors='coerce').fillna(0)
    kpi_merged = kpi_merged.sort_values(by='RS', ascending=False).reset_index(drop=True)
    kdq_merged = kdq_merged.sort_values(by='RS', ascending=False).reset_index(drop=True)
    
    return kpi_merged, kdq_merged

def calculate_growth(row):
    """ 타임라인 역추적 로직: 최근 4분기/3년 YoY 성장률 계산 """
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
                        # 분기 말일 날짜 매핑
                        md = {'1Q':'03-31', '2Q':'06-30', '3Q':'09-30', '4Q':'12-31', '1Y':'12-31'}.get(q, '12-31')
                        dt = pd.to_datetime(f"{y}-{md}")
                        growth_data.append({'Date': dt, 'Period': c, 'Growth': g_rate})
            except: pass
        return pd.DataFrame(growth_data)

    q_growth = get_yoy(q_cols, 4)
    y_growth = get_yoy(y_cols, 3)
    return q_growth, y_growth

def draw_stock_chart(row):
    """ Plotly 하이엔드 차트 렌더링 엔진 (이중축, 7/8 스케일, 한글 툴팁) """
    sym, name, rs = row['종목코드'], row.get('종목명', ''), row.get('RS', 0)
    
    # 1. 주가 데이터 로드 (1.5년 치 확보 후 1년 치 슬라이싱)
    end_date = datetime.date.today()
    start_fetch = end_date - datetime.timedelta(days=500)
    start_view = end_date - datetime.timedelta(days=365)
    
    try:
        df_price = fdr.DataReader(sym, start_fetch, end_date)
        if df_price.empty: return go.Figure().update_layout(title="주가 데이터 없음")
    except: return go.Figure().update_layout(title="주가 API 로드 실패")

    df_price['SMA50'] = df_price['Close'].rolling(window=50).mean()
    df_price['SMA150'] = df_price['Close'].rolling(window=150).mean()
    df_price['SMA200'] = df_price['Close'].rolling(window=200).mean()
    df_view = df_price[df_price.index >= pd.to_datetime(start_view)]
    
    # 2. 서브플롯 뼈대 생성 (1/5 높이의 거래량, 이중 Y축)
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, 
        row_heights=[0.8, 0.2], vertical_spacing=0.03,
        specs=[[{"secondary_y": True}], [{"secondary_y": False}]]
    )
    
    # 3. 캔들차트 및 이평선 (오른쪽 Y축: 가격)
    hover_candle = "날짜: %{x|%Y-%m-%d}<br>시가: %{open:,.0f}원<br>고가: %{high:,.0f}원<br>저가: %{low:,.0f}원<br>종가: %{close:,.0f}원<extra></extra>"
    fig.add_trace(go.Candlestick(
        x=df_view.index, open=df_view['Open'], high=df_view['High'], 
        low=df_view['Low'], close=df_view['Close'], name='일봉',
        increasing_line_color='red', decreasing_line_color='blue', hovertemplate=hover_candle
    ), row=1, col=1, secondary_y=True)

    for sma, color in zip(['SMA50', 'SMA150', 'SMA200'], ['orange', 'purple', 'gray']):
        fig.add_trace(go.Scatter(x=df_view.index, y=df_view[sma], name=sma, line=dict(color=color, width=1.5), hoverinfo='skip'), row=1, col=1, secondary_y=True)

    # 4. 성장률 그래프 (왼쪽 Y축: %) - 중앙부 배치
    q_growth, y_growth = calculate_growth(row)
    hover_growth = "결산일: %{x|%Y-%m-%d}<br>기간: %{text}<br>증감률: %{y:.2f}%<extra></extra>"
    
    if not q_growth.empty:
        q_growth = q_growth[q_growth['Date'] >= pd.to_datetime(start_view)] 
        if not q_growth.empty:
            fig.add_trace(go.Scatter(
                x=q_growth['Date'], y=q_growth['Growth'], text=q_growth['Period'], name='분기 증감률',
                mode='lines+markers', line=dict(color='cyan', width=2, dash='dot'), marker=dict(size=10, symbol='diamond'),
                hovertemplate=hover_growth
            ), row=1, col=1, secondary_y=False)

    if not y_growth.empty:
        y_growth = y_growth[y_growth['Date'] >= pd.to_datetime(start_view)]
        if not y_growth.empty:
            fig.add_trace(go.Scatter(
                x=y_growth['Date'], y=y_growth['Growth'], text=y_growth['Period'], name='연간 증감률',
                mode='lines+markers', line=dict(color='magenta', width=2), marker=dict(size=12, symbol='star'),
                hovertemplate=hover_growth
            ), row=1, col=1, secondary_y=False)

    # 5. 거래량 차트 (하단)
    colors = ['red' if c >= o else 'blue' for c, o in zip(df_view['Close'], df_view['Open'])]
    hover_vol = "날짜: %{x|%Y-%m-%d}<br>거래량: %{y:,}주<extra></extra>"
    fig.add_trace(go.Bar(
        x=df_view.index, y=df_view['Volume'], marker_color=colors, name='거래량', hovertemplate=hover_vol
    ), row=2, col=1)

    # 6. 스케일링 및 레이아웃 제어 (최고점 7/8 스케일링)
    max_price = df_view['High'].max()
    max_vol = df_view['Volume'].max()
    
    fig.update_layout(
        title=dict(text=f"<b>{name}</b> ({sym}) | RS: {rs:.1f}", font=dict(size=18), x=0.02),
        xaxis=dict(range=[start_view, end_date], rangeslider=dict(visible=False), type='date'),
        yaxis=dict(title="성장률 (%)", side="left", showgrid=False, fixedrange=True, range=[-200, 200]), 
        yaxis2=dict(title="주가 (원)", side="right", fixedrange=True, range=[df_view['Low'].min() * 0.9, max_price * (8/7)]),
        yaxis3=dict(fixedrange=True, range=[0, max_vol * (8/7)]), 
        legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="right", x=1, font=dict(size=10)),
        margin=dict(l=40, r=40, t=60, b=20),
        height=450,
        hovermode='x unified'
    )
    
    return fig

# ==========================================
# 🚀 메인 대시보드 UI 및 그리드 렌더링
# ==========================================
st.sidebar.title("🧭 시장 선택")
market = st.sidebar.radio("트렌드 템플릿 선택", ("KOSPI (코스피)", "KOSDAQ (코스닥)"))

try:
    kpi_df, kdq_df = load_data()
    target_df = kpi_df if "KOSPI" in market else kdq_df
    
    if target_df.empty:
        st.warning("선택한 시장의 정상 종목 데이터가 없습니다. URL을 확인하세요.")
        st.stop()
        
    st.sidebar.markdown(f"**검출된 종목:** 총 {len(target_df)}개")
    
    # 4개 단위 페이징 (2x2 그리드)
    items_per_page = 4
    total_pages = (len(target_df) // items_per_page) + (1 if len(target_df) % items_per_page > 0 else 0)
    page_num = st.sidebar.number_input(f"페이지 이동 (1 ~ {total_pages})", min_value=1, max_value=total_pages, value=1)
    
    start_idx = (page_num - 1) * items_per_page
    view_df = target_df.iloc[start_idx:start_idx + items_per_page]
    
    # 2x2 그리드 렌더링
    st.markdown("<style> .stPlotlyChart {border-radius: 10px; box-shadow: 2px 2px 10px rgba(0,0,0,0.1);} </style>", unsafe_allow_html=True)
    
    for i in range(0, len(view_df), 2):
        cols = st.columns(2) 
        
        with cols[0]:
            if i < len(view_df):
                fig1 = draw_stock_chart(view_df.iloc[i])
                st.plotly_chart(fig1, use_container_width=True)
                
        with cols[1]:
            if i+1 < len(view_df):
                fig2 = draw_stock_chart(view_df.iloc[i+1])
                st.plotly_chart(fig2, use_container_width=True)

except Exception as e:
    st.error(f"대시보드 초기화 실패: {e}")
