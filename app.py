import streamlit as st
import pandas as pd
import numpy as np
import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import FinanceDataReader as fdr
import gspread

# ⚙️ 페이지 설정 (전체화면, 사이드바 기본 확장)
st.set_page_config(page_title="Quant Vertical Screener", layout="wide", initial_sidebar_state="expanded")

# 🚨 [사용자 입력 필수] 본인의 구글 시트 URL 3개를 정확히 입력하세요!
URL_MASTER_DB = '여기에_마스터DB_주소를_입력하세요'
URL_SOURCE_KOSPI = '여기에_코스피_트렌드템플릿_주소를_입력하세요'
URL_SOURCE_KOSDAQ = '여기에_코스닥_트렌드템플릿_주소를_입력하세요'

@st.cache_data(ttl=3600)
def load_data():
    """ 3개의 시트를 로드하고 조인하여 퀀트 유니버스를 생성 """
    
    # 🚨 Streamlit Secrets 기반 인증
    credentials_dict = dict(st.secrets["gcp_service_account"])
    gc = gspread.service_account_from_dict(credentials_dict)
    
    def get_df(url):
        sheet = gc.open_by_url(url).sheet1
        data = sheet.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        # 종목코드 규격화 (6자리 문자열)
        code_col = next((c for c in df.columns if '코드' in c or 'Code' in c or '종목' in c or 'ticker' in c.lower()), None)
        if code_col:
            df['종목코드'] = df[code_col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.zfill(6)
        return df

    master_df = get_df(URL_MASTER_DB)
    kospi_df = get_df(URL_SOURCE_KOSPI)
    kosdaq_df = get_df(URL_SOURCE_KOSDAQ)
    
    # 마스터 DB 정상 종목만 숫자형으로 변환 및 종목명 확보
    master_df = master_df[master_df['데이터_상태'].str.contains('✅ 정상', na=False)].copy()
    
    # 🎯 [피드백 반영] 종목이름 열 확보 (없으면 종목코드로 대체)
    name_col = next((c for c in master_df.columns if '이름' in c or '명' in c or 'Name' in c), None)
    if not name_col:
        master_df['종목명'] = master_df['종목코드']
    else:
        master_df['종목명'] = master_df[name_col]
        
    time_cols = [c for c in master_df.columns if '_' in c]
    for c in time_cols:
        master_df[c] = pd.to_numeric(master_df[c].astype(str).str.replace(',', ''), errors='coerce')
        
    # RS 열 찾기
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
                        md = {'1Q':'03-31', '2Q':'06-30', '3Q':'09-30', '4Q':'12-31', '1Y':'12-31'}.get(q, '12-31')
                        dt = pd.to_datetime(f"{y}-{md}")
                        growth_data.append({'Date': dt, 'Period': c, 'Growth': g_rate})
            except: pass
        return pd.DataFrame(growth_data)

    q_growth = get_yoy(q_cols, 4)
    y_growth = get_yoy(y_cols, 3)
    return q_growth, y_growth

def draw_stock_chart(row):
    """ Plotly 하이엔드 차트 렌더링 엔진 (다이나믹 스케일링, 한글 툴팁) """
    sym, name, rs = row['종목코드'], row.get('종목명', ''), row.get('RS', 0)
    
    # 1. 주가 데이터 로드
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
    
    # 뼈대 생성
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, 
        row_heights=[0.8, 0.2], vertical_spacing=0.03,
        specs=[[{"secondary_y": True}], [{"secondary_y": False}]]
    )
    
    # 3. 캔들차트 및 이평선 (우측 가격 Y축)
    fig.add_trace(go.Candlestick(
        x=df_view.index, open=df_view['Open'], high=df_view['High'], 
        low=df_view['Low'], close=df_view['Close'], name='일봉',
        increasing_line_color='red', decreasing_line_color='blue'
    ), row=1, col=1, secondary_y=True)

    for sma, color in zip(['SMA50', 'SMA150', 'SMA200'], ['orange', 'purple', 'gray']):
        fig.add_trace(go.Scatter(x=df_view.index, y=df_view[sma], name=sma, line=dict(color=color, width=1.5)), row=1, col=1, secondary_y=True)

    # 4. 성장률 그래프 (좌측 % Y축) - 다이나믹 스케일링 준비
    q_growth, y_growth = calculate_growth(row)
    
    # 🎯 [피드백 반영] 성장률 Y축 다이나믹 스케일링을 위한 데이터 수집
    growth_values = []
    
    if not q_growth.empty:
        q_growth = q_growth[q_growth['Date'] >= pd.to_datetime(start_view)] 
        if not q_growth.empty:
            fig.add_trace(go.Scatter(
                x=q_growth['Date'], y=q_growth['Growth'], text=q_growth['Period'], name='분기 증감률',
                mode='lines+markers', line=dict(color='cyan', width=2, dash='dot'), marker=dict(size=10, symbol='diamond')
            ), row=1, col=1, secondary_y=False)
            growth_values.extend(q_growth['Growth'].tolist())

    if not y_growth.empty:
        y_growth = y_growth[y_growth['Date'] >= pd.to_datetime(start_view)]
        if not y_growth.empty:
            fig.add_trace(go.Scatter(
                x=y_growth['Date'], y=y_growth['Growth'], text=y_growth['Period'], name='연간 증감률',
                mode='lines+markers', line=dict(color='magenta', width=2), marker=dict(size=12, symbol='star')
            ), row=1, col=1, secondary_y=False)
            growth_values.extend(y_growth['Growth'].tolist())

    # 🎯 [피드백 반영] 성장률 Y축 다이나믹 자동 범위 계산 (Padding 10% 추가)
    if growth_values:
        g_min = min(growth_values)
        g_max = max(growth_values)
        g_range = g_max - g_min if g_max != g_min else 100
        y_left_min = g_min - (g_range * 0.1)
        y_left_max = g_max + (g_range * 0.1)
        fixed_y_left = False # 자동 범위 사용
    else:
        y_left_min, y_left_max = -100, 100
        fixed_y_left = False

    # 5. 거래량 차트
    fig.add_trace(go.Bar(
        x=df_view.index, y=df_view['Volume'], marker_color='gray', name='거래량'
    ), row=2, col=1)

    # 6. 레이아웃 제어 (스케일링 적용, 주가 Y축 7/8 스케일)
    max_price = df_view['High'].max()
    max_vol = df_view['Volume'].max()
    
    fig.update_layout(
        # 🎯 [피드백 반영] 차트 제목 종목명으로 변경
        title=dict(text=f"<b>{name}</b> ({sym}) | RS: {rs:.1f}", font=dict(size=18), x=0.02),
        xaxis=dict(range=[start_view, end_date], rangeslider=dict(visible=False), type='date'),
        
        # 🎯 [피드백 반영] 좌측 Y축 다이나믹 범위 설정
        yaxis=dict(title="성장률 (%)", side="left", showgrid=False, fixedrange=True, range=[y_left_min, y_left_max]), 
        
        yaxis2=dict(title="주가 (원)", side="right", fixedrange=True, range=[df_view['Low'].min() * 0.9, max_price * (8/7)]),
        yaxis3=dict(fixedrange=True, range=[0, max_vol * (8/7)]), 
        legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="right", x=1, font=dict(size=10)),
        margin=dict(l=40, r=40, t=60, b=20),
        height=450,
        hovermode='x unified'
    )
    
    # 🎯 [피드백 반영] 한글 툴팁(hover) 강제 매핑
    fig.update_traces(hovertemplate="날짜: %{x|%Y-%m-%d}<br>시가: %{open:,.0f}원<br>고가: %{high:,.0f}원<br>저가: %{low:,.0f}원<br>종가: %{close:,.0f}원<extra></extra>", selector=dict(type="candlestick"))
    fig.update_traces(hovertemplate="결산일: %{x|%Y-%m-%d}<br>기간: %{text}<br>증감률: %{y:.2f}%<extra></extra>", selector=dict(mode="lines+markers"))
    fig.update_traces(hovertemplate="날짜: %{x|%Y-%m-%d}<br>거래량: %{y:,}주<extra></extra>", selector=dict(type="bar"))
    
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
    
    items_per_page = 2
    total_pages = (len(target_df) // items_per_page) + (1 if len(target_df) % items_per_page > 0 else 0)
    page_num = st.sidebar.number_input(f"페이지 이동 (1 ~ {total_pages})", min_value=1, max_value=total_pages, value=1)
    
    start_idx = (page_num - 1) * items_per_page
    view_df = target_df.iloc[start_idx:start_idx + items_per_page]
    
    st.markdown("<style> .stPlotlyChart {border-radius: 10px; box-shadow: 2px 2px 10px rgba(0,0,0,0.1); margin-bottom: 20px;} </style>", unsafe_allow_html=True)
    
    for _, row in view_df.iterrows():
        fig = draw_stock_chart(row)
        st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error("🚨 앗! 데이터 연결 또는 렌더링 중 문제가 발생했습니다. 아래의 추적 로그(엑스레이)를 확인하세요.")
    st.exception(e) # 👈 핵심: 에러의 엑스레이를 화면에 적나라하게 띄움
