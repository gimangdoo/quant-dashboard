import streamlit as st
import pandas as pd
import numpy as np
import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import FinanceDataReader as fdr
import gspread

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
    
    # 🎯 [핵심 패치 2] 무결점 다중 방어막이 탑재된 KRX 공식 상장종목 API 호출
    try:
        krx_df = fdr.StockListing('KRX')
        # 방어 1: fdr 최신 버전은 'Symbol', 구버전은 'Code'를 사용함. 둘 다 완벽히 대응.
        code_col_krx = 'Symbol' if 'Symbol' in krx_df.columns else 'Code'
        krx_dict = dict(zip(krx_df[code_col_krx].astype(str).str.zfill(6), krx_df['Name']))
        
        kpi_merged['종목명'] = kpi_merged['종목코드'].map(krx_dict).fillna(kpi_merged['종목코드'])
        kdq_merged['종목명'] = kdq_merged['종목코드'].map(krx_dict).fillna(kdq_merged['종목코드'])
        
    except Exception as e:
        # 방어 2: 클라우드 서버 IP 차단 또는 API 완전 마비 시 GitHub 백업 CSV 파일로 긴급 우회(Fallback)
        try:
            backup_df = pd.read_csv("https://raw.githubusercontent.com/corazzon/finance-data-analysis/main/krx.csv")
            code_col_bk = 'Symbol' if 'Symbol' in backup_df.columns else 'Code'
            krx_dict = dict(zip(backup_df[code_col_bk].astype(str).str.zfill(6), backup_df['Name']))
            
            kpi_merged['종목명'] = kpi_merged['종목코드'].map(krx_dict).fillna(kpi_merged['종목코드'])
            kdq_merged['종목명'] = kdq_merged['종목코드'].map(krx_dict).fillna(kdq_merged['종목코드'])
        except:
            # 최악의 경우에만 코드 번호 출력
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

def draw_stock_chart(row, view_mode):
    sym, name, rs = row['종목코드'], row.get('종목명', ''), row.get('RS', 0)
    
    end_date = datetime.date.today()
    
    if view_mode == "📱 모바일 모드 (최근 1년 줌인)":
        start_view = end_date - datetime.timedelta(days=365)
    else:
        start_view = end_date - datetime.timedelta(days=1095) # PC 3년 파노라마
        
    start_fetch = start_view - datetime.timedelta(days=300) # SMA200 선확보
    
    try:
        df_price = fdr.DataReader(sym, start_fetch, end_date)
        if df_price.empty: return go.Figure().update_layout(title="주가 데이터 없음")
    except: return go.Figure().update_layout(title="주가 API 로드 실패")

    df_price['SMA50'] = df_price['Close'].rolling(window=50).mean()
    df_price['SMA150'] = df_price['Close'].rolling(window=150).mean()
    df_price['SMA200'] = df_price['Close'].rolling(window=200).mean()
    
    df_view = df_price[df_price.index >= pd.to_datetime(start_view)].reset_index()
    df_view['idx'] = np.arange(len(df_view))
    
    df_view['YearMonth'] = df_view['Date'].dt.strftime('%Y-%m')
    first_days = df_view.drop_duplicates(subset=['YearMonth'], keep='first')
    tickvals = first_days['idx'].tolist()
    ticktext = [f"{d.year}년 {d.month}월" if d.month == 1 else f"{d.month}월" for d in first_days['Date']]

    # 서브플롯 뼈대 생성 (Shared X-axis)
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, 
        row_heights=[0.8, 0.2], vertical_spacing=0.03,
        specs=[[{"secondary_y": True}], [{"secondary_y": False}]]
    )
    
    # 1. 🎯 [핵심 패치] 캔들차트 및 이평선 스타일 개조 (Ultra-Thin Border & High Contrast)
    fig.add_trace(go.Candlestick(
        x=df_view['idx'], open=df_view['Open'], high=df_view['High'], 
        low=df_view['Low'], close=df_view['Close'], name='일봉',
        
        # 🎯 경계선을 날카롭게 (초박형 외곽선 설정)
        increasing_line_color='#FF4136', # 더 선명한 고화질 전용 Red
        increasing_line_width=1,          # 외곽선 두께를 1px로 고정 (뭉개짐 방지)
        increasing_fillcolor='#FF4136',    # 내부 꽉 채움
        
        decreasing_line_color='#0074D9', # 더 선명한 고화질 전용 Blue
        decreasing_line_width=1,          # 외곽선 두께를 1px로 고정
        decreasing_fillcolor='#0074D9',    # 내부 꽉 채움
        
        opacity=1.0,                     # 투명도 없음 (선명도 극대화)
        customdata=df_view['Date'].dt.strftime('%Y-%m-%d'),
        hovertemplate="날짜: %{customdata}<br>시가: %{open:,.0f}원<br>고가: %{high:,.0f}원<br>저가: %{low:,.0f}원<br>종가: %{close:,.0f}원<extra></extra>"
    ), row=1, col=1, secondary_y=True)

    # 이평선 선 두께도 약간 줄여 미니멀리즘 구현
    for sma, color in zip(['SMA50', 'SMA150', 'SMA200'], ['orange', 'purple', 'gray']):
        fig.add_trace(go.Scatter(
            x=df_view['idx'], y=df_view[sma], name=sma, 
            line=dict(color=color, width=1.0), hoverinfo='skip' # 선 두께 1.5 -> 1.0
        ), row=1, col=1, secondary_y=True)

    # 2. 성장률 그래프
    q_growth, y_growth = calculate_growth(row)
    growth_values = []
    
    df_view_sorted = df_view[['Date', 'idx']].sort_values('Date')

    if not q_growth.empty:
        q_growth = q_growth[q_growth['Date'] >= pd.to_datetime(start_view)].sort_values('Date')
        if not q_growth.empty:
            q_growth = pd.merge_asof(q_growth, df_view_sorted, on='Date', direction='nearest')
            fig.add_trace(go.Scatter(
                x=q_growth['idx'], y=q_growth['Growth'], text=q_growth['Period'], name='분기 증감률',
                mode='lines+markers', line=dict(color='cyan', width=1.5, dash='dot'), marker=dict(size=8, symbol='diamond'), # 크기 약간 축소
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
                mode='lines+markers', line=dict(color='magenta', width=1.5), marker=dict(size=10, symbol='star'), # 크기 약간 축소
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

    # 3. 거래량 차트 (선명도 상승)
    fig.add_trace(go.Bar(
        x=df_view['idx'], y=df_view['Volume'], 
        marker_color='gray', opacity=0.8, name='거래량', # 투명도 주어 배경과 분리
        customdata=df_view['Date'].dt.strftime('%Y-%m-%d'),
        hovertemplate="날짜: %{customdata}<br>거래량: %{y:,}주<extra></extra>"
    ), row=2, col=1)

    # 4. 🎯 [핵심 패치 2] 레이아웃 미니멀리즘 (그리드 제거 및 대비 상승)
    max_price = df_view['High'].max()
    max_vol = df_view['Volume'].max()
    
    # 🎯 모든 X축 세로 눈금선 제거 및 선명도 상승
    fig.update_xaxes(
        showticklabels=False, 
        showgrid=False, # 세로 격자 과감히 제거 (가독성 폭발)
        zeroline=False, 
        row=1, col=1
    )
    
    # 하단 X축 라벨도 선명하게
    fig.update_xaxes(
        tickmode='array', tickvals=tickvals, ticktext=ticktext, showticklabels=True, 
        showgrid=False, # 세로 격자 제거
        zeroline=False,
        row=2, col=1
    )
    
    # 🎯 Y축 가로 눈금선 선명도 조정
    fig.update_yaxes(
        showgrid=True, gridwidth=0.5, gridcolor='#F0F0F0', # 아주 희미한 가로 guides만 남김
        zeroline=False,
        row=1, col=1
    )
    fig.update_yaxes(
        showgrid=False, # 하단 거래량 격자 제거
        zeroline=False,
        row=2, col=1
    )
    
    fig.update_layout(
        title=dict(text=f"<b>{name}</b> ({sym}) | RS: {rs:.1f}", font=dict(size=18, color='black'), x=0.02),
        xaxis=dict(rangeslider=dict(visible=False)),
        
        # 🎯 주가/거래량 Y축 7/8 스케일링
        yaxis=dict(title="성장률 (%)", side="left", showgrid=False, fixedrange=True, range=[y_left_min, y_left_max]), 
        yaxis2=dict(title="주가 (원)", side="right", fixedrange=True, range=[df_view['Low'].min() * 0.9, max_price * (8/7)]),
        yaxis3=dict(fixedrange=True, range=[0, max_vol * (8/7)]), 
        
        # 🎯 [핵심 패치 3] 미니멀리즘 배경 및 Hover 스타일 (High Contrast)
        plot_bgcolor='white', # 깨끗한 백색 배경으로 대비 극대화
        paper_bgcolor='white',
        hovermode='x',        # unified보다 'x'가 좁은 화면에서 덜 번져 보임
        
        legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="right", x=1, font=dict(size=10)),
        margin=dict(l=40, r=40, t=60, b=20),
        height=450
    )
    
    return fig

# ==========================================
# 🚀 메인 대시보드 UI 및 그리드 렌더링
# ==========================================
st.sidebar.title("🧭 시장 선택")
market = st.sidebar.radio("트렌드 템플릿 선택", ("KOSPI (코스피)", "KOSDAQ (코스닥)"))

st.sidebar.markdown("---")
view_mode = st.sidebar.radio("🖥️ 화면 모드", ("💻 PC 모드 (최근 3년 파노라마)", "📱 모바일 모드 (최근 1년 줌인)"))

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
        fig = draw_stock_chart(row, view_mode)
        st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error("🚨 앗! 데이터 연결 또는 렌더링 중 문제가 발생했습니다.")
    st.exception(e)
