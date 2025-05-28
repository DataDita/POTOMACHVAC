import streamlit as st
from snowflake.snowpark import Session
import snowflake.snowpark as sp
from datetime import datetime, timedelta, time
import pandas as pd
import re
import uuid
import hashlib
import base64
from PIL import Image, ImageOps
import io
import requests
import os
import traceback

# Initialize Snowflake connection
def get_session():
    return sp.context.get_active_session()

# Role-based access control
ROLE_ACCESS = {
    'admin': ['Home', 'profile', 'customers', 'appointments', 'quotes', 'invoices', 'payments', 'reports', 'analytics', 'admin_tables', 'equipment'],
    'office': ['Home', 'customers', 'appointments', 'equipment'],
    'technician': ['Home', 'profile', 'quotes', 'invoices', 'payments', 'equipment'],
    'driver': ['Home', 'profile', 'driver_tasks']
}

def login_page():
    st.title("POTOMAC HVAC")
    emp_id = st.text_input("Employee ID")
    password = st.text_input("Password", type='password')
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Login"):
            try:
                session = get_session()
                result = session.sql("""
                    SELECT e.*, r.ROLENAME
                    FROM EMPLOYEES e
                    JOIN EMPLOYEE_ROLES er ON e.EMPLOYEEID = er.EMPLOYEEID
                    JOIN ROLES r ON er.ROLEID = r.ROLEID
                    WHERE e.EMPLOYEEID = ? AND e.PASSWORD = ?
                """, params=[emp_id, password]).collect()
                
                if result:
                    st.session_state.update({
                        'logged_in': True,
                        'user_id': emp_id,
                        'user_name': result[0]['ENAME'],
                        'roles': [row['ROLENAME'] for row in result]
                    })
                    st.rerun()
                else:
                    st.error("Invalid credentials")
            except Exception as e:
                st.error(f"Login error: {str(e)}")
                st.error(traceback.format_exc())
    with col2:
        if st.button("Forgot Password?"):
            st.session_state['show_forgot_password'] = True

    if st.session_state.get('show_forgot_password'):
        forgot_password()
        if st.button("Back to Login"):
            st.session_state['show_forgot_password'] = False
            st.rerun()

def forgot_password():
    st.subheader("🔒 Forgot Password")
    email = st.text_input("Enter your email address")
    if st.button("Send Reset Link"):
        try:
            session = get_session()
            employee = session.sql("""
                SELECT EMPLOYEEID FROM EMPLOYEES
                WHERE EMAIL = ?
            """, params=[email]).collect()
            
            if employee:
                employee_id = employee[0]['EMPLOYEEID']
                reset_token = str(uuid.uuid4())
                token_hash = hashlib.sha256(reset_token.encode()).hexdigest()
                expires_at = datetime.now() + timedelta(hours=1)
                
                session.sql("""
                    INSERT INTO PASSWORD_RESETS
                    (RESETID, EMPLOYEEID, RESET_TOKEN, EXPIRES_AT)
                    VALUES (?, ?, ?, ?)
                """, params=[str(uuid.uuid4()), employee_id, token_hash, expires_at]).collect()
                
                st.success("Password reset link sent to your email!")
            else:
                st.error("No account found with that email address")
        except Exception as e:
            st.error(f"Error processing request: {str(e)}")
            st.error(traceback.format_exc())

def reset_password(token):
    st.subheader("🔑 Reset Password")
    new_password = st.text_input("New Password", type='password')
    confirm_password = st.text_input("Confirm Password", type='password')
    if st.button("Reset Password"):
        if new_password != confirm_password:
            st.error("Passwords do not match")
            return
            
        try:
            session = get_session()
            token_hash = hashlib.sha256(token.encode()).hexdigest()
            reset_record = session.sql("""
                SELECT * FROM PASSWORD_RESETS
                WHERE RESET_TOKEN = ?
                AND USED = FALSE
                AND EXPIRES_AT > CURRENT_TIMESTAMP()
            """, params=[token_hash]).collect()
            
            if reset_record:
                employee_id = reset_record[0]['EMPLOYEEID']
                session.sql("""
                    UPDATE EMPLOYEES
                    SET PASSWORD = ?
                    WHERE EMPLOYEEID = ?
                """, params=[new_password, employee_id]).collect()
                
                session.sql("""
                    UPDATE PASSWORD_RESETS
                    SET USED = TRUE
                    WHERE RESETID = ?
                """, params=[reset_record[0]['RESETID']]).collect()
                
                st.success("Password reset successfully!")
                st.session_state.clear()
                st.rerun()
            else:
                st.error("Invalid or expired reset token")
        except Exception as e:
            st.error(f"Error resetting password: {str(e)}")
            st.error(traceback.format_exc())

def Home():
    try:
        session = get_session()
        selected_date = datetime.now().date()
        manual_time = False
        manual_clock_in = None
        manual_clock_out = None
        manual_break_start = None
        manual_break_end = None
        is_clocked_in = False
        is_on_break = False

        st.title("Home")
        
        col1, col2 = st.columns([4, 1])
        with col1:
            st.subheader(f"Welcome, {st.session_state.user_name}!")
            st.caption(f"Role: {', '.join(st.session_state.roles)}")

        with col2:
            pic_data = session.sql("""
                SELECT PICTURE_DATA_TEXT FROM EMPLOYEE_PICTURES
                WHERE EMPLOYEEID = ?
                ORDER BY UPLOADED_AT DESC LIMIT 1
            """, params=[st.session_state.user_id]).collect()
            
            if pic_data and pic_data[0]['PICTURE_DATA_TEXT']:
                img = Image.open(io.BytesIO(base64.b64decode(pic_data[0]['PICTURE_DATA_TEXT'])))
                st.image(img, width=100)
            else:
                st.image(Image.new('RGB', (100,100), color='gray'), width=100)

        st.subheader("Time Tracking")
        manual_time = st.toggle("Manual Time Entry", help="Enable to enter times manually")
        
        time_options = []
        for hour in range(7, 23):
            for minute in [0, 15, 30, 45]:
                time_obj = time(hour, minute)
                time_options.append((time_obj.strftime("%I:%M %p"), time_obj))
        
        if manual_time:
            with st.form("manual_time_form", clear_on_submit=False):
                st.warning("Manual Entry Mode - For correcting missed punches")
                
                selected_date = st.date_input(
                    "Entry Date",
                    value=datetime.now().date(),
                    min_value=datetime.now().date() - timedelta(days=30),
                    max_value=datetime.now().date()
                )
                
                cols = st.columns(2)
                with cols[0]:
                    selected_clock_in = st.selectbox(
                        "Clock In Time",
                        options=[t[0] for t in time_options],
                        index=0,
                        key="clock_in_select"
                    )
                    manual_clock_in = next(t[1] for t in time_options if t[0] == selected_clock_in)
                
                with cols[1]:
                    selected_clock_out = st.selectbox(
                        "Clock Out Time",
                        options=["Not clocked out yet"] + [t[0] for t in time_options],
                        index=0,
                        key="clock_out_select"
                    )
                    manual_clock_out = next((t[1] for t in time_options if t[0] == selected_clock_out), None)
                
                cols = st.columns(2)
                with cols[0]:
                    selected_break_start = st.selectbox(
                        "Break Start",
                        options=["No break"] + [t[0] for t in time_options],
                        index=0,
                        key="break_start_select"
                    )
                    manual_break_start = next((t[1] for t in time_options if t[0] == selected_break_start), None)
                
                with cols[1]:
                    selected_break_end = st.selectbox(
                        "Break End",
                        options=["Break not ended"] + [t[0] for t in time_options],
                        index=0,
                        key="break_end_select"
                    )
                    manual_break_end = next((t[1] for t in time_options if t[0] == selected_break_end), None)
                
                if st.form_submit_button("Save Manual Entry"):
                    if not manual_clock_in:
                        st.error("Clock in time is required")
                    else:
                        try:
                            clock_in_dt = datetime.combine(selected_date, manual_clock_in)
                            clock_out_dt = datetime.combine(selected_date, manual_clock_out) if manual_clock_out else None
                            
                            existing = session.sql("""
                                SELECT ENTRYID FROM EMPLOYEE_TIME_ENTRIES
                                WHERE EMPLOYEEID = ?
                                AND ENTRY_DATE = ?
                                LIMIT 1
                            """, params=[st.session_state.user_id, selected_date]).collect()
                            
                            if existing:
                                session.sql("""
                                    UPDATE EMPLOYEE_TIME_ENTRIES
                                    SET CLOCK_IN = ?,
                                        CLOCK_OUT = ?
                                    WHERE ENTRYID = ?
                                """, params=[clock_in_dt, clock_out_dt, existing[0]['ENTRYID']]).collect()
                            else:
                                entry_id = f"ENTRY{datetime.now().timestamp()}"
                                session.sql("""
                                    INSERT INTO EMPLOYEE_TIME_ENTRIES
                                    (ENTRYID, EMPLOYEEID, CLOCK_IN, CLOCK_OUT, ENTRY_DATE)
                                    VALUES (?, ?, ?, ?, ?)
                                """, params=[entry_id, st.session_state.user_id, clock_in_dt, clock_out_dt, selected_date]).collect()
                            
                            if manual_break_start and manual_break_end:
                                break_start_dt = datetime.combine(selected_date, manual_break_start)
                                break_end_dt = datetime.combine(selected_date, manual_break_end)
                                
                                existing_break = session.sql("""
                                    SELECT BREAKID FROM EMPLOYEE_BREAK_ENTRIES
                                    WHERE EMPLOYEEID = ?
                                    AND ENTRY_DATE = ?
                                    LIMIT 1
                                """, params=[st.session_state.user_id, selected_date]).collect()
                                
                                if existing_break:
                                    session.sql("""
                                        UPDATE EMPLOYEE_BREAK_ENTRIES
                                        SET BREAK_START = ?,
                                            BREAK_END = ?
                                        WHERE BREAKID = ?
                                    """, params=[break_start_dt, break_end_dt, existing_break[0]['BREAKID']]).collect()
                                else:
                                    break_id = f"BREAK{datetime.now().timestamp()}"
                                    session.sql("""
                                        INSERT INTO EMPLOYEE_BREAK_ENTRIES
                                        (BREAKID, EMPLOYEEID, BREAK_START, BREAK_END, ENTRY_DATE)
                                        VALUES (?, ?, ?, ?, ?)
                                    """, params=[break_id, st.session_state.user_id, break_start_dt, break_end_dt, selected_date]).collect()
                            
                            st.success("Time entry saved successfully!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error saving time entry: {str(e)}")
                            st.error(traceback.format_exc())
        else:
            time_entry = session.sql("""
                SELECT * FROM EMPLOYEE_TIME_ENTRIES
                WHERE EMPLOYEEID = ?
                AND ENTRY_DATE = ?
                ORDER BY CLOCK_IN DESC
                LIMIT 1
            """, params=[st.session_state.user_id, selected_date]).collect()
            
            break_entry = session.sql("""
                SELECT * FROM EMPLOYEE_BREAK_ENTRIES
                WHERE EMPLOYEEID = ?
                AND ENTRY_DATE = ?
                ORDER BY BREAK_START DESC
                LIMIT 1
            """, params=[st.session_state.user_id, selected_date]).collect()
            
            is_clocked_in = len(time_entry) > 0 and time_entry[0]['CLOCK_OUT'] is None
            is_on_break = len(break_entry) > 0 and break_entry[0]['BREAK_END'] is None
            
            cols = st.columns(2)
            with cols[0]:
                if st.button("🟢 Clock In", disabled=is_clocked_in):
                    session.sql("""
                        INSERT INTO EMPLOYEE_TIME_ENTRIES
                        (ENTRYID, EMPLOYEEID, CLOCK_IN, ENTRY_DATE)
                        VALUES (?, ?, CURRENT_TIMESTAMP(), ?)
                    """, params=[f'ENTRY{datetime.now().timestamp()}', st.session_state.user_id, selected_date]).collect()
                    st.rerun()
            
            with cols[1]:
                if st.button("🔴 Clock Out", disabled=not is_clocked_in or is_on_break):
                    session.sql("""
                        UPDATE EMPLOYEE_TIME_ENTRIES
                        SET CLOCK_OUT = CURRENT_TIMESTAMP()
                        WHERE EMPLOYEEID = ?
                        AND ENTRY_DATE = ?
                        AND CLOCK_OUT IS NULL
                    """, params=[st.session_state.user_id, selected_date]).collect()
                    st.rerun()
            
            cols = st.columns(2)
            with cols[0]:
                if st.button("🟡 Start Break", disabled=not is_clocked_in or is_on_break):
                    session.sql("""
                        INSERT INTO EMPLOYEE_BREAK_ENTRIES
                        (BREAKID, EMPLOYEEID, BREAK_START, ENTRY_DATE)
                        VALUES (?, ?, CURRENT_TIMESTAMP(), ?)
                    """, params=[f'BREAK{datetime.now().timestamp()}', st.session_state.user_id, selected_date]).collect()
                    st.rerun()
            
            with cols[1]:
                if st.button("🟢 End Break", disabled=not is_on_break):
                    session.sql("""
                        UPDATE EMPLOYEE_BREAK_ENTRIES
                        SET BREAK_END = CURRENT_TIMESTAMP()
                        WHERE EMPLOYEEID = ?
                        AND ENTRY_DATE = ?
                        AND BREAK_END IS NULL
                    """, params=[st.session_state.user_id, selected_date]).collect()
                    st.rerun()

        time_entries = session.sql("""
            SELECT CLOCK_IN, CLOCK_OUT 
            FROM EMPLOYEE_TIME_ENTRIES
            WHERE EMPLOYEEID = ?
            AND ENTRY_DATE = ?
            ORDER BY CLOCK_IN
        """, params=[st.session_state.user_id, selected_date]).collect()
        
        break_entries = session.sql("""
            SELECT BREAK_START, BREAK_END 
            FROM EMPLOYEE_BREAK_ENTRIES
            WHERE EMPLOYEEID = ?
            AND ENTRY_DATE = ?
            ORDER BY BREAK_START
        """, params=[st.session_state.user_id, selected_date]).collect()

        if time_entries:
            total_seconds = 0
            for entry in time_entries:
                if entry['CLOCK_OUT']:
                    total_seconds += (entry['CLOCK_OUT'] - entry['CLOCK_IN']).total_seconds()
                else:
                    total_seconds += (datetime.now() - entry['CLOCK_IN']).total_seconds()
            
            break_seconds = 0
            for entry in break_entries:
                if entry['BREAK_END']:
                    break_seconds += (entry['BREAK_END'] - entry['BREAK_START']).total_seconds()
            
            net_seconds = total_seconds - break_seconds
            hours = int(net_seconds // 3600)
            minutes = int((net_seconds % 3600) // 60)
            
            time_str = f"{hours} hour{'s' if hours != 1 else ''} {minutes} minute{'s' if minutes != 1 else ''}"
            st.markdown("---")
            st.metric("Total Worked Today", time_str)
            st.markdown("---")

        st.header("📅 Upcoming Appointments")
        
        appointments = session.sql("""
            SELECT 
                a.APPOINTMENTID,
                a.SERVICE_TYPE,
                a.SCHEDULED_TIME,
                a.STA_TUS,
                c.NAME AS CUSTOMER_NAME,
                c.ADDRESS,
                c.UNIT,
                c.CITY,
                c.STATE,
                c.ZIPCODE,
                c.HAS_LOCK_BOX,
                c.LOCK_BOX_CODE,
                c.HAS_SAFETY_ALARM,
                c.SAFETY_ALARM,
                c.ENTRANCE_NOTE,
                c.NOTE,
                c.UNIT_LOCATION,
                c.ACCESSIBILITY_LEVEL,
                c.PHONE,
                c.EMAIL
            FROM APPOINTMENTS a
            JOIN CUSTOMERS c ON a.CUSTOMERID = c.CUSTOMERID
            WHERE a.TECHNICIANID = ?
            AND a.SCHEDULED_TIME BETWEEN CURRENT_TIMESTAMP() 
                AND DATEADD('day', 7, CURRENT_TIMESTAMP())
            ORDER BY a.SCHEDULED_TIME
        """, params=[st.session_state.user_id]).collect()

        if not appointments:
            st.info("No upcoming appointments in the next 7 days")
            return

        for appt in appointments:
            col1, col2, col3, col4 = st.columns([1,1,3,1])
            
            with col1:
                st.markdown(f"**{appt['SERVICE_TYPE'].capitalize()}**")
            
            with col2:
                dt = appt['SCHEDULED_TIME']
                st.write(f"{dt.strftime('%a %m/%d')}\n{dt.strftime('%I:%M %p')}")
            
            with col3:
                full_address = f"{appt['ADDRESS']}, {appt['CITY']}, {appt['STATE']} {appt['ZIPCODE']}"
                maps_url = f"https://www.google.com/maps/search/?api=1&query={full_address.replace(' ', '+')}"
                
                with st.expander(f"**{appt['CUSTOMER_NAME']}** - {appt['ADDRESS']}, {appt['CITY']}"):
                    st.markdown(f"**Complete Address:** [📌 {full_address}]({maps_url})")
                    st.markdown(f"""
                        **Unit #:** {appt['UNIT'] or 'N/A'}  
                        **Lock Box Code:** {appt['LOCK_BOX_CODE'] if appt['HAS_LOCK_BOX'] == 'Yes' else 'N/A'}  
                        **Safety Alarm:** {appt['SAFETY_ALARM'] if appt['HAS_SAFETY_ALARM'] == 'Yes' else 'N/A'}  
                        **Entrance Notes:** {appt['ENTRANCE_NOTE'] or 'N/A'}  
                        **General Notes:** {appt['NOTE'] or 'N/A'}  
                        **Unit Location:** {appt['UNIT_LOCATION']}  
                        **Accessibility:** {appt['ACCESSIBILITY_LEVEL']}  
                        **Phone:** {appt['PHONE']}  
                        **Email:** {appt['EMAIL'] or 'N/A'}
                    """)

            with col4:
                current_status = appt['STA_TUS'].lower()
                status_colors = {
                    'scheduled': '#4a4a4a',
                    'accepted': '#2e7d32',
                    'declined': '#c62828',
                    'arrived': '#1565c0'
                }
                
                st.markdown(f"""
                    <div style="
                        background-color: {status_colors.get(current_status, '#4a4a4a')};
                        color: white;
                        padding: 0.5rem;
                        border-radius: 0.5rem;
                        text-align: center;
                        margin: 0.5rem 0;
                        font-size: 0.9rem;
                    ">
                        {current_status.capitalize()}
                    </div>
                """, unsafe_allow_html=True)
                
                if current_status == 'scheduled':
                    if st.button("✅ Accept", key=f"accept_{appt['APPOINTMENTID']}"):
                        session.sql("""
                            UPDATE APPOINTMENTS
                            SET STA_TUS = 'accepted'
                            WHERE APPOINTMENTID = ?
                        """, params=[appt['APPOINTMENTID']]).collect()
                        st.rerun()
                    
                    if st.button("❌ Decline", key=f"decline_{appt['APPOINTMENTID']}"):
                        session.sql("""
                            UPDATE APPOINTMENTS
                            SET STA_TUS = 'declined'
                            WHERE APPOINTMENTID = ?
                        """, params=[appt['APPOINTMENTID']]).collect()
                        st.rerun()
                
                elif current_status == 'accepted':
                    if st.button("📍 I'm Here", key=f"arrived_{appt['APPOINTMENTID']}"):
                        session.sql("""
                            UPDATE APPOINTMENTS
                            SET STA_TUS = 'arrived'
                            WHERE APPOINTMENTID = ?
                        """, params=[appt['APPOINTMENTID']]).collect()
                        st.rerun()

            st.markdown("---")
            
    except Exception as e:
        st.error(f"Home page error: {str(e)}")
        st.error(traceback.format_exc())

def profile_page():
    try:
        session = get_session()
        
        col1, col2 = st.columns([1, 4])
        
        with col1:
            pic_data = session.sql("""
                SELECT PICTURE_DATA_TEXT FROM EMPLOYEE_PICTURES
                WHERE EMPLOYEEID = ?
                ORDER BY UPLOADED_AT DESC LIMIT 1
            """, params=[st.session_state.user_id]).collect()
            
            if pic_data and pic_data[0]['PICTURE_DATA_TEXT']:
                img_data = base64.b64decode(pic_data[0]['PICTURE_DATA_TEXT'])
                img = Image.open(io.BytesIO(img_data))
                img.thumbnail((80, 80), Image.Resampling.LANCZOS)
                st.image(img, width=80)
            else:
                st.image(Image.new('RGB', (80, 80), color='lightgray'))
            
            with st.expander("🖼"):
                uploaded_file = st.file_uploader("", type=["jpg", "jpeg", "png"], key="pic_uploader")
                if uploaded_file and st.button("Update", key="pic_update"):
                    try:
                        img = Image.open(uploaded_file)
                        img = ImageOps.fit(img, (500, 500))
                        buffer = io.BytesIO()
                        img.save(buffer, format="JPEG", quality=90)
                        encoded_image = base64.b64encode(buffer.getvalue()).decode('utf-8')
                        
                        session.sql("""
                            INSERT INTO EMPLOYEE_PICTURES 
                            (PICTUREID, EMPLOYEEID, PICTURE_DATA_TEXT)
                            VALUES (?, ?, ?)
                        """, params=[f'PIC{datetime.now().timestamp()}', st.session_state.user_id, encoded_image]).collect()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

        with col2:
            st.title(f"{st.session_state.user_name}'s Profile")
            st.caption(f"Employee ID: {st.session_state.user_id}")

        today = datetime.now().date()
        start_of_week = today - timedelta(days=today.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        
        with st.expander("📅 Date Range", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("From", value=start_of_week)
            with col2:
                end_date = st.date_input("To", value=end_of_week, min_value=start_date)

        tab1, tab2, tab3, tab4 = st.tabs([
            "📅 Schedule", 
            "⏱ Work History", 
            "💰 Earnings", 
            "📝 Appointments"
        ])

        with tab1:
            employee_name = session.sql("""
                SELECT ENAME FROM EMPLOYEES
                WHERE EMPLOYEEID = ?
            """, params=[st.session_state.user_id]).collect()[0]['ENAME']
            
            schedules = session.sql("""
                SELECT * FROM EMPLOYEE_SCHEDULES
                WHERE EMPLOYEEID = ?
                AND SCHEDULE_DATE BETWEEN ? AND ?
                ORDER BY SCHEDULE_DATE, START_TIME
            """, params=[st.session_state.user_id, start_date, end_date]).collect()
            
            st.markdown("""
            <style>
                .employee-box {
                    display: inline-block;
                    background-color: #e6f7ff;
                    border-radius: 4px;
                    padding: 2px 6px;
                    margin: 2px;
                    font-size: 12px;
                    border: 1px solid #b3e0ff;
                }
                .schedule-table {
                    width: 100%;
                    border-collapse: collapse;
                    font-size: 14px;
                }
                .schedule-table th, .schedule-table td {
                    border: 1px solid #ddd;
                    padding: 8px;
                    text-align: center;
                }
                .schedule-table th {
                    background-color: #f2f2f2;
                    font-weight: bold;
                }
                .time-col {
                    background-color: #f9f9f9;
                    font-weight: bold;
                }
            </style>
            """, unsafe_allow_html=True)
            
            time_slots = [
                ("8:00-10:00", time(8, 0), time(10, 0)),
                ("10:00-12:00", time(10, 0), time(12, 0)),
                ("12:00-14:00", time(12, 0), time(14, 0)),
                ("14:00-16:00", time(14, 0), time(16, 0)),
                ("16:00-18:00", time(16, 0), time(18, 0))
            ]
            
            days = [(start_date + timedelta(days=i)).strftime("%a %m/%d") 
                   for i in range((end_date - start_date).days + 1)]
            day_dates = [start_date + timedelta(days=i) 
                        for i in range((end_date - start_date).days + 1)]
            
            table_html = """
            <table class="schedule-table">
                <tr>
                    <th>Time Slot</th>
            """
            for day in days:
                table_html += f"<th>{day}</th>"
            table_html += "</tr>"
            
            for slot_name, slot_start, slot_end in time_slots:
                table_html += f"<tr><td class='time-col'>{slot_name}</td>"
                
                for day_date in day_dates:
                    scheduled = False
                    for s in schedules:
                        if s['SCHEDULE_DATE'] == day_date:
                            s_start = s['START_TIME']
                            s_end = s['END_TIME']
                            if (s_start < slot_end) and (s_end > slot_start):
                                scheduled = True
                                break
                    
                    table_html += "<td>"
                    if scheduled:
                        table_html += f"<div class='employee-box'>{employee_name}</div>"
                    table_html += "</td>"
                
                table_html += "</tr>"
            
            table_html += "</table>"
            st.markdown(table_html, unsafe_allow_html=True)

        with tab2:
            time_entries = session.sql("""
                SELECT 
                    ENTRY_DATE,
                    CLOCK_IN,
                    CLOCK_OUT,
                    TIMEDIFF('MINUTE', CLOCK_IN, CLOCK_OUT)/60.0 as HOURS_WORKED
                FROM EMPLOYEE_TIME_ENTRIES
                WHERE EMPLOYEEID = ?
                AND ENTRY_DATE BETWEEN ? AND ?
                ORDER BY ENTRY_DATE DESC, CLOCK_IN DESC
            """, params=[st.session_state.user_id, start_date, end_date]).collect()
            
            if time_entries:
                total_hours = sum(entry['HOURS_WORKED'] or 0 for entry in time_entries)
                
                st.dataframe(
                    pd.DataFrame([{
                        "Date": e['ENTRY_DATE'].strftime('%Y-%m-%d'),
                        "Clock In": e['CLOCK_IN'].strftime('%I:%M %p') if e['CLOCK_IN'] else "-",
                        "Clock Out": e['CLOCK_OUT'].strftime('%I:%M %p') if e['CLOCK_OUT'] else "-",
                        "Hours": f"{e['HOURS_WORKED']:.2f}" if e['HOURS_WORKED'] else "-"
                    } for e in time_entries]),
                    hide_index=True,
                    use_container_width=True
                )
                
                st.metric("Total Hours Worked", f"{total_hours:.2f}")
            else:
                st.info("No work history for selected period")

        with tab3:
            emp_rate = session.sql("""
                SELECT HOURLYRATE FROM EMPLOYEES
                WHERE EMPLOYEEID = ?
            """, params=[st.session_state.user_id]).collect()[0]['HOURLYRATE']
            
            earnings = session.sql("""
                SELECT 
                    ENTRY_DATE,
                    SUM(TIMEDIFF('MINUTE', CLOCK_IN, CLOCK_OUT)/60.0) as HOURS_WORKED
                FROM EMPLOYEE_TIME_ENTRIES
                WHERE EMPLOYEEID = ?
                AND CLOCK_OUT IS NOT NULL
                AND ENTRY_DATE BETWEEN ? AND ?
                GROUP BY ENTRY_DATE
                ORDER BY ENTRY_DATE DESC
            """, params=[st.session_state.user_id, start_date, end_date]).collect()
            
            if earnings:
                total_earnings = sum(e['HOURS_WORKED'] * emp_rate for e in earnings)
                
                st.dataframe(
                    pd.DataFrame([{
                        "Date": e['ENTRY_DATE'].strftime('%Y-%m-%d'),
                        "Hours": f"{e['HOURS_WORKED']:.2f}",
                        "Rate": f"${emp_rate:.2f}",
                        "Earnings": f"${e['HOURS_WORKED'] * emp_rate:.2f}"
                    } for e in earnings]),
                    hide_index=True,
                    use_container_width=True
                )
                
                st.metric("Total Earnings", f"${total_earnings:.2f}")
            else:
                st.info("No earnings data for selected period")

        with tab4:
            appointments = session.sql("""
                SELECT 
                    c.NAME as CUSTOMER,
                    a.SCHEDULED_TIME,
                    TO_VARCHAR(a.SCHEDULED_TIME, 'HH12:MI AM') as TIME,
                    a.STA_TUS as STATUS,
                    a.NOTES
                FROM APPOINTMENTS a
                JOIN CUSTOMERS c ON a.CUSTOMERID = c.CUSTOMERID
                WHERE a.TECHNICIANID = ?
                AND DATE(a.SCHEDULED_TIME) BETWEEN ? AND ?
                ORDER BY a.SCHEDULED_TIME
            """, params=[st.session_state.user_id, start_date, end_date]).collect()
            
            if appointments:
                for appt in appointments:
                    with st.expander(f"{appt['SCHEDULED_TIME'].strftime('%a %m/%d')} - {appt['CUSTOMER']} ({appt['TIME']})"):
                        st.write(f"**Status:** {appt['STATUS'].capitalize()}")
                        if appt['NOTES']:
                            st.write(f"**Notes:** {appt['NOTES']}")
            else:
                st.info("No appointments for selected period")
                
    except Exception as e:
        st.error(f"Profile page error: {str(e)}")
        st.error(traceback.format_exc())

def customer_management():
    try:
        session = get_session()
        st.subheader("👥 Customer Management")

        if 'customer_form_data' not in st.session_state:
            st.session_state.customer_form_data = {
                'name': '',
                'phone': '',
                'email': '',
                'address': '',
                'unit': '',
                'city': '',
                'state': 'MD',
                'zipcode': '',
                'has_lock_box': 'No',
                'lock_box_code': '',
                'has_safety_alarm': 'No',
                'safety_alarm_code': '',
                'how_heard': '',
                'friend_name': '',
                'note': '',
                'entrance_note': '',
                'outdoor_unit_model': '',
                'outdoor_unit_serial': '',
                'indoor_unit_model': '',
                'indoor_unit_serial': '',
                'thermostat_type': '',
                'unit_location': 'Attic',
                'accessibility_level': 'Easy',
            }

        with st.expander("➕ Add New Customer", expanded=False):
            with st.form(key="add_customer_form", clear_on_submit=True):
                col1, col2 = st.columns(2)
                
                with col1:
                    name = st.text_input("Full Name*", value=st.session_state.customer_form_data['name'])
                    phone = st.text_input("Phone* (###-###-####)", value=st.session_state.customer_form_data['phone'], placeholder="301-555-1234")
                    email = st.text_input("Email", value=st.session_state.customer_form_data['email'])
                    address = st.text_input("Street Address*", value=st.session_state.customer_form_data['address'])
                    unit = st.text_input("Unit/Apt", value=st.session_state.customer_form_data['unit'])
                    
                with col2:
                    city = st.text_input("City*", value=st.session_state.customer_form_data['city'])
                    state = st.selectbox("State*", ["MD", "DC", "VA"], index=["MD", "DC", "VA"].index(st.session_state.customer_form_data['state']))
                    zipcode = st.text_input("Zip Code* (5 or 9 digits)", value=st.session_state.customer_form_data['zipcode'])
                    
                    how_heard = st.selectbox(
                        "How did you hear about us?",
                        ["", "Google", "Friend", "Facebook", "Yelp", "Other"],
                        index=0
                    )
                    friend_name = ""
                    if how_heard == "Friend":
                        friend_name = st.text_input("Friend's Name*", value=st.session_state.customer_form_data['friend_name'])
                
                col1, col2 = st.columns(2)
                with col1:
                    has_lock_box = st.radio("Lock Box", ["No", "Yes"], 
                                          index=0 if st.session_state.customer_form_data['has_lock_box'] == 'No' else 1, 
                                          horizontal=True)
                    if has_lock_box == "Yes":
                        lock_box_code = st.text_input("Lock Box Code*", value=st.session_state.customer_form_data['lock_box_code'])
                
                with col2:
                    has_safety_alarm = st.radio("Safety Alarm", ["No", "Yes"], 
                                              index=0 if st.session_state.customer_form_data['has_safety_alarm'] == 'No' else 1, 
                                              horizontal=True)
                    if has_safety_alarm == "Yes":
                        safety_alarm_code = st.text_input("Safety Alarm Code*", value=st.session_state.customer_form_data['safety_alarm_code'])
                
                st.subheader("Equipment Information")
                col1, col2 = st.columns(2)
                with col1:
                    outdoor_unit_model = st.text_input("Outdoor Unit Model", value=st.session_state.customer_form_data['outdoor_unit_model'])
                    outdoor_unit_serial = st.text_input("Outdoor Unit Serial Number", value=st.session_state.customer_form_data['outdoor_unit_serial'])
                    indoor_unit_model = st.text_input("Indoor Unit Model", value=st.session_state.customer_form_data['indoor_unit_model'])
                    indoor_unit_serial = st.text_input("Indoor Unit Serial Number", value=st.session_state.customer_form_data['indoor_unit_serial'])
                    
                with col2:
                    thermostat_type = st.text_input("Thermostat Type", value=st.session_state.customer_form_data['thermostat_type'])
                    unit_location = st.selectbox(
                        "Unit Location",
                        ["Attic", "Basement", "Garage", "Closet", "Crawlspace", "Other"],
                        index=["Attic", "Basement", "Garage", "Closet", "Crawlspace", "Other"].index(
                            st.session_state.customer_form_data['unit_location'])
                    )
                    accessibility_level = st.selectbox(
                        "Accessibility Level",
                        ["Easy", "Moderate", "Difficult", "Very Difficult"],
                        index=["Easy", "Moderate", "Difficult", "Very Difficult"].index(
                            st.session_state.customer_form_data['accessibility_level'])
                    )
                
                st.subheader("Unit Pictures")
                uploaded_files = st.file_uploader("Upload pictures of the HVAC unit", 
                                                accept_multiple_files=True, 
                                                type=['jpg', 'jpeg', 'png'])
                
                note = st.text_area("General Notes", value=st.session_state.customer_form_data['note'])
                entrance_note = st.text_area("Entrance Notes", value=st.session_state.customer_form_data['entrance_note'])

                col1, col2 = st.columns(2)
                with col1:
                    submitted = st.form_submit_button("Add Customer")
                with col2:
                    if st.form_submit_button("Cancel"):
                        st.session_state.customer_form_data = {
                            'name': '',
                            'phone': '',
                            'email': '',
                            'address': '',
                            'unit': '',
                            'city': '',
                            'state': 'MD',
                            'zipcode': '',
                            'has_lock_box': 'No',
                            'lock_box_code': '',
                            'has_safety_alarm': 'No',
                            'safety_alarm_code': '',
                            'how_heard': '',
                            'friend_name': '',
                            'note': '',
                            'entrance_note': '',
                            'outdoor_unit_model': '',
                            'outdoor_unit_serial': '',
                            'indoor_unit_model': '',
                            'indoor_unit_serial': '',
                            'thermostat_type': '',
                            'unit_location': 'Attic',
                            'accessibility_level': 'Easy'
                        }
                        st.rerun()
                
                if submitted:
                    st.session_state.customer_form_data.update({
                        'name': name,
                        'phone': phone,
                        'email': email,
                        'address': address,
                        'unit': unit,
                        'city': city,
                        'state': state,
                        'zipcode': zipcode,
                        'has_lock_box': has_lock_box,
                        'lock_box_code': lock_box_code if has_lock_box == "Yes" else '',
                        'has_safety_alarm': has_safety_alarm,
                        'safety_alarm_code': safety_alarm_code if has_safety_alarm == "Yes" else '',
                        'how_heard': how_heard,
                        'friend_name': friend_name if how_heard == "Friend" else '',
                        'note': note,
                        'entrance_note': entrance_note,
                        'outdoor_unit_model': outdoor_unit_model,
                        'outdoor_unit_serial': outdoor_unit_serial,
                        'indoor_unit_model': indoor_unit_model,
                        'indoor_unit_serial': indoor_unit_serial,
                        'thermostat_type': thermostat_type,
                        'unit_location': unit_location,
                        'accessibility_level': accessibility_level
                    })

                    errors = []
                    if not name:
                        errors.append("Full Name is required")
                    if not phone:
                        errors.append("Phone is required")
                    elif not re.match(r"^\d{3}-\d{3}-\d{4}$", phone):
                        errors.append("Invalid phone format (use ###-###-####)")
                    if not address:
                        errors.append("Address is required")
                    if not city:
                        errors.append("City is required")
                    if not state:
                        errors.append("State is required")
                    if not zipcode:
                        errors.append("Zip Code is required")
                    elif not re.match(r"^\d{5}(-\d{4})?$", zipcode):
                        errors.append("Invalid zip code format (use 5 or 9 digits)")
                    if email and not re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", email):
                        errors.append("Invalid email format")
                    if how_heard == "Friend" and not friend_name:
                        errors.append("Friend's Name is required when referral is from friend")
                    if has_lock_box == "Yes" and not lock_box_code:
                        errors.append("Lock Box Code is required when Lock Box is set to Yes")
                    if has_safety_alarm == "Yes" and not safety_alarm_code:
                        errors.append("Safety Alarm Code is required when Safety Alarm is set to Yes")

                    if errors:
                        for error in errors:
                            st.error(error)
                    else:
                        try:
                            last_customer = session.sql("""
                                SELECT CUSTOMERID FROM CUSTOMERS 
                                ORDER BY CUSTOMERID DESC 
                                LIMIT 1
                            """).collect()
                            
                            if last_customer:
                                last_id = last_customer[0]['CUSTOMERID']
                                if last_id.startswith('CU'):
                                    try:
                                        next_num = int(last_id[2:]) + 1
                                        customer_id = f"CU{next_num}"
                                    except:
                                        customer_id = "CU100"
                                else:
                                    customer_id = "CU100"
                            else:
                                customer_id = "CU100"
                            
                            how_heard_value = how_heard
                            if how_heard == "Friend":
                                how_heard_value = f"Friend: {friend_name}"
                            
                            session.sql("""
                                INSERT INTO CUSTOMERS 
                                (CUSTOMERID, NAME, PHONE, EMAIL, ADDRESS, UNIT, CITY, STATE, ZIPCODE,
                                 HAS_LOCK_BOX, LOCK_BOX_CODE, HAS_SAFETY_ALARM, SAFETY_ALARM, HOW_HEARD, 
                                 NOTE, ENTRANCE_NOTE, OUTDOOR_UNIT_MODEL, OUTDOOR_UNIT_SERIAL_NUMBER,
                                 INDOOR_UNIT_MODEL, INDOOR_UNIT_SERIAL_NUMBER, THERMOSTAT_TYPE,
                                 UNIT_LOCATION, ACCESSIBILITY_LEVEL, ACCESSIBILITY_NOTES, OTHER_NOTES)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, params=[
                                customer_id,
                                name,
                                phone,
                                email,
                                address,
                                unit,
                                city,
                                state,
                                zipcode,
                                has_lock_box,
                                lock_box_code if has_lock_box == "Yes" else None,
                                has_safety_alarm,
                                safety_alarm_code if has_safety_alarm == "Yes" else None,
                                how_heard_value,
                                note,
                                entrance_note,
                                outdoor_unit_model,
                                outdoor_unit_serial,
                                indoor_unit_model,
                                indoor_unit_serial,
                                thermostat_type,
                                unit_location,
                                accessibility_level,
                                '',
                                ''
                            ]).collect()
                            
                            if uploaded_files:
                                for uploaded_file in uploaded_files:
                                    file_data = uploaded_file.read()
                                    encoded_file = base64.b64encode(file_data).decode('utf-8')
                                    session.sql("""
                                        INSERT INTO CUSTOMER_DOCUMENTS 
                                        (DOC_ID, CUSTOMERID, DOC_TYPE, DESCRIPTION, DOC_DATA)
                                        VALUES (?, ?, 'IMAGE', ?, ?)
                                    """, params=[
                                        f'DOC{datetime.now().timestamp()}',
                                        customer_id,
                                        f'Unit Picture - {uploaded_file.name}',
                                        encoded_file
                                    ]).collect()
                            
                            st.success(f"✅ Customer added successfully! Customer ID: {customer_id}")
                            st.session_state.customer_form_data = {
                                'name': '',
                                'phone': '',
                                'email': '',
                                'address': '',
                                'unit': '',
                                'city': '',
                                'state': 'MD',
                                'zipcode': '',
                                'has_lock_box': 'No',
                                'lock_box_code': '',
                                'has_safety_alarm': 'No',
                                'safety_alarm_code': '',
                                'how_heard': '',
                                'friend_name': '',
                                'note': '',
                                'entrance_note': '',
                                'outdoor_unit_model': '',
                                'outdoor_unit_serial': '',
                                'indoor_unit_model': '',
                                'indoor_unit_serial': '',
                                'thermostat_type': '',
                                'unit_location': 'Attic',
                                'accessibility_level': 'Easy'
                            }
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error adding customer: {str(e)}")
                            st.error(traceback.format_exc())

        st.subheader("🔍 Search Customers")
        search_term = st.text_input("", placeholder="Search by name, phone, email, or address", key="unified_search")
        
        if search_term:
            search_pattern = f"%{search_term}%"
            customers = session.sql("""
                SELECT c.* FROM CUSTOMERS c
                WHERE c.NAME ILIKE ?
                   OR c.PHONE ILIKE ?
                   OR c.EMAIL ILIKE ?
                   OR c.ADDRESS ILIKE ?
                ORDER BY c.NAME
            """, params=[search_pattern, search_pattern, search_pattern, search_pattern]).collect()
            
            if customers:
                for customer in customers:
                    try:
                        customer_dict = customer.as_dict() if hasattr(customer, 'as_dict') else dict(zip(customer._fields, customer))
                    except:
                        customer_dict = dict(zip(['CUSTOMERID', 'NAME', 'PHONE', 'EMAIL', 'ADDRESS', 'UNIT', 'CITY', 'STATE', 'ZIPCODE',
                                                'HAS_LOCK_BOX', 'LOCK_BOX_CODE', 'HAS_SAFETY_ALARM', 'SAFETY_ALARM', 'HOW_HEARD',
                                                'CREATED_AT', 'LAST_QUOTE_ID', 'LAST_QUOTE_DATE', 'NOTE', 'ENTRANCE_NOTE',
                                                'OUTDOOR_UNIT_MODEL', 'OUTDOOR_UNIT_SERIAL_NUMBER', 'INDOOR_UNIT_MODEL', 
                                                'INDOOR_UNIT_SERIAL_NUMBER', 'THERMOSTAT_TYPE', 'UNIT_LOCATION', 
                                                'ACCESSIBILITY_LEVEL', 'ACCESSIBILITY_NOTES', 'OTHER_NOTES'], customer))
                    
                    with st.container():
                        st.subheader(f"{customer_dict['NAME']} - {customer_dict['PHONE']}")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write(f"**Customer ID:** {customer_dict['CUSTOMERID']}")
                            st.write(f"**Email:** {customer_dict.get('EMAIL', 'Not provided')}")
                            
                            full_address = f"{customer_dict['ADDRESS']}, {customer_dict['CITY']}, {customer_dict['STATE']} {customer_dict['ZIPCODE']}"
                            maps_url = f"https://www.google.com/maps/search/?api=1&query={full_address.replace(' ', '+')}"
                            st.markdown(f"""
                                **Address:** <a href="{maps_url}" target="_blank" style="color: blue; text-decoration: none;">
                                {customer_dict['ADDRESS']}{', ' + customer_dict['UNIT'] if customer_dict.get('UNIT') else ''}<br>
                                {customer_dict['CITY']}, {customer_dict['STATE']} {customer_dict['ZIPCODE']}.
                                </a>
                            """, unsafe_allow_html=True)
                            
                        with col2:
                            has_lock_box = customer_dict.get('HAS_LOCK_BOX', 'No')
                            lock_box_code = st.text_input(
                                "Lock Box Code",
                                value=customer_dict.get('LOCK_BOX_CODE', ''),
                                disabled=(has_lock_box != 'Yes'),
                                key=f"lock_box_{customer_dict['CUSTOMERID']}"
                            )
                            
                            has_safety_alarm = customer_dict.get('HAS_SAFETY_ALARM', 'No')
                            safety_alarm_code = st.text_input(
                                "Safety Alarm Code",
                                value=customer_dict.get('SAFETY_ALARM', ''),
                                disabled=(has_safety_alarm != 'Yes'),
                                key=f"safety_alarm_{customer_dict['CUSTOMERID']}"
                            )
                        
                        st.write(f"**How Heard:** {customer_dict.get('HOW_HEARD', 'Not specified')}")
                        st.write(f"**General Note:** {customer_dict.get('NOTE', 'None')}")
                        st.write(f"**Entrance Note:** {customer_dict.get('ENTRANCE_NOTE', 'None')}")
                        
                        st.subheader("📅 Customer Appointments")
                        appointments = session.sql("""
                            SELECT a.*, e.ENAME as TECHNICIAN_NAME 
                            FROM APPOINTMENTS a
                            JOIN EMPLOYEES e ON a.TECHNICIANID = e.EMPLOYEEID
                            WHERE a.CUSTOMERID = ?
                            ORDER BY a.SCHEDULED_TIME DESC
                        """, params=[customer_dict['CUSTOMERID']]).collect()
                        
                        if appointments:
                            for appt in appointments:
                                cols = st.columns([1,2,1])
                                with cols[0]:
                                    st.write(f"**{appt['SCHEDULED_TIME'].strftime('%Y-%m-%d %I:%M %p')}**")
                                with cols[1]:
                                    st.write(f"{appt['SERVICE_TYPE']} ({appt['STA_TUS']})")
                                    st.write(f"Technician: {appt['TECHNICIAN_NAME']}")
                                with cols[2]:
                                    if st.button("View Details", key=f"appt_details_{appt['APPOINTMENTID']}"):
                                        st.session_state['view_appt'] = appt['APPOINTMENTID']
                        else:
                            st.info("No appointments scheduled for this customer")
                        
                        st.subheader("Equipment Information")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Outdoor Unit:** {customer_dict.get('OUTDOOR_UNIT_MODEL', 'N/A')}")
                            st.write(f"**Serial #:** {customer_dict.get('OUTDOOR_UNIT_SERIAL_NUMBER', 'N/A')}")
                            st.write(f"**Indoor Unit:** {customer_dict.get('INDOOR_UNIT_MODEL', 'N/A')}")
                            st.write(f"**Serial #:** {customer_dict.get('INDOOR_UNIT_SERIAL_NUMBER', 'N/A')}")
                        with col2:
                            st.write(f"**Thermostat:** {customer_dict.get('THERMOSTAT_TYPE', 'N/A')}")
                            st.write(f"**Location:** {customer_dict.get('UNIT_LOCATION', 'N/A')}")
                            st.write(f"**Accessibility:** {customer_dict.get('ACCESSIBILITY_LEVEL', 'N/A')}")
                        
                        st.subheader("Unit Pictures")
                        docs = session.sql("""
                        SELECT DOC_ID, DESCRIPTION, UPLOADED_AT 
                        FROM CUSTOMER_DOCUMENTS 
                        WHERE CUSTOMERID = ?
                        AND DOC_TYPE = 'IMAGE'
                        ORDER BY UPLOADED_AT DESC
                        """, params=[customer_dict['CUSTOMERID']]).collect()

                        if docs:
                            cols = st.columns(min(3, len(docs)))
                            for i, doc in enumerate(docs):
                                with cols[i % 3]:
                                    img_data = session.sql("""
                                    SELECT DOC_DATA FROM CUSTOMER_DOCUMENTS
                                    WHERE DOC_ID = ?
                                    """, params=[doc['DOC_ID']]).collect()[0]['DOC_DATA']
                                    
                                    if img_data:
                                        try:
                                            img = Image.open(io.BytesIO(base64.b64decode(img_data)))
                                            st.image(img, 
                                                     caption=doc['DESCRIPTION'], 
                                                     use_container_width=True)
                                            st.caption(f"{doc['UPLOADED_AT'].strftime('%Y-%m-%d')}")
                                        except:
                                            st.error("Could not display image")
                        else:
                            st.info("No unit pictures available")

                        col1, col2, col3 = st.columns(3)
                        with col1:
                            if st.button("Edit", key=f"edit_{customer_dict['CUSTOMERID']}"):
                                st.session_state['edit_customer'] = customer_dict['CUSTOMERID']
                                st.session_state['customer_to_edit'] = customer_dict
                                st.rerun()
                        with col2:
                            if st.button("Schedule Appointment", key=f"appt_{customer_dict['CUSTOMERID']}"):
                                st.session_state['selected_customer_id'] = customer_dict['CUSTOMERID']
                                st.session_state['selected_customer_name'] = customer_dict['NAME']
                                st.rerun()
                        with col3:
                            if st.button("Upload Picture", key=f"pic_{customer_dict['CUSTOMERID']}"):
                                st.session_state['add_picture_customer'] = customer_dict['CUSTOMERID']
                                st.rerun()

        if 'add_picture_customer' in st.session_state:
            st.subheader(f"Add Unit Picture for Customer {st.session_state['add_picture_customer']}")
            
            uploaded_file = st.file_uploader("Upload picture of the HVAC unit", type=['jpg', 'jpeg', 'png'])
            description = st.text_input("Picture Description")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Save Picture"):
                    if uploaded_file:
                        try:
                            file_data = uploaded_file.read()
                            encoded_file = base64.b64encode(file_data).decode('utf-8')
                            session.sql("""
                                INSERT INTO CUSTOMER_DOCUMENTS 
                                (DOC_ID, CUSTOMERID, DOC_TYPE, DESCRIPTION, DOC_DATA)
                                VALUES (?, ?, 'IMAGE', ?, ?)
                            """, params=[
                                f'DOC{datetime.now().timestamp()}',
                                st.session_state['add_picture_customer'],
                                description,
                                encoded_file
                            ]).collect()
                            st.success("Picture added successfully!")
                            del st.session_state['add_picture_customer']
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error saving picture: {str(e)}")
                    else:
                        st.error("Please select a file to upload")
            with col2:
                if st.button("Cancel"):
                    del st.session_state['add_picture_customer']
                    st.rerun()

        if 'selected_customer_id' in st.session_state and 'selected_customer_name' in st.session_state:
            st.subheader(f"📅 Schedule Appointment for {st.session_state['selected_customer_name']}")
            
            request_type = st.selectbox(
                "Select Request Type",
                ["Repair", "Maintenance", "Install", "Estimate"],
                index=0
            )
            
            is_urgent = st.radio("Urgent?", ["No", "Yes"], horizontal=True)
            
            expertise_map = {
                "Repair": "EX1", 
                "Maintenance": "EX2", 
                "Install": "EX3", 
                "Estimate": "EX4"
            }
            technicians = session.sql("""
                SELECT e.EMPLOYEEID, e.ENAME 
                FROM EMPLOYEES e
                JOIN EMPLOYEE_EXPERTISE ee ON e.EMPLOYEEID = ee.EMPLOYEEID
                WHERE ee.EXPERTISEID = ?
            """, params=[expertise_map[request_type]]).collect()
            
            if not technicians:
                st.error("No technicians available for this service type")
            else:
                if request_type == "Install":
                    st.subheader("Select Installation Date")
                    
                    start_date = datetime.now().date()
                    dates = [start_date + timedelta(days=i) for i in range(28)]
                    
                    booked_days = session.sql("""
                        SELECT DISTINCT DATE(SCHEDULED_TIME) as DAY 
                        FROM APPOINTMENTS 
                        WHERE SERVICE_TYPE = 'Install'
                        AND DATE(SCHEDULED_TIME) BETWEEN ? AND ?
                    """, params=[start_date, start_date + timedelta(days=28)]).collect()
                    booked_days = [row['DAY'] for row in booked_days]
                    
                    cols = st.columns(7)
                    for i, date in enumerate(dates):
                        with cols[i % 7]:
                            if date in booked_days:
                                st.button(
                                    f"{date.strftime('%a %m/%d')}",
                                    disabled=True,
                                    key=f"install_day_{date}"
                                )
                            else:
                                if st.button(
                                    f"{date.strftime('%a %m/%d')}",
                                    key=f"install_day_{date}"
                                ):
                                    st.session_state.selected_install_date = date
                    
                    if 'selected_install_date' in st.session_state:
                        date = st.session_state.selected_install_date
                        st.success(f"Selected installation date: {date.strftime('%A, %B %d')}")
                        
                        primary_tech = st.selectbox(
                            "Primary Technician",
                            options=[t['EMPLOYEEID'] for t in technicians],
                            format_func=lambda x: next(t['ENAME'] for t in technicians if t['EMPLOYEEID'] == x))
                        
                        secondary_techs = [t for t in technicians if t['EMPLOYEEID'] != primary_tech]
                        secondary_tech = st.selectbox(
                            "Additional Technician (Optional)",
                            options=[""] + [t['EMPLOYEEID'] for t in secondary_techs],
                            format_func=lambda x: next(t['ENAME'] for t in technicians if t['EMPLOYEEID'] == x) if x else "None"
                        )
                        
                        notes = st.text_area("Installation Notes")
                        if is_urgent == "Yes":
                            notes = "URGENT: " + notes
                        
                        if st.button("Book Installation"):
                            try:
                                session.sql("""
                                    INSERT INTO APPOINTMENTS (
                                        APPOINTMENTID, CUSTOMERID, TECHNICIANID,
                                        SCHEDULED_TIME, SERVICE_TYPE, NOTES, STA_TUS
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                """, params=[
                                    f'APT{datetime.now().timestamp()}',
                                    st.session_state['selected_customer_id'],
                                    primary_tech,
                                    datetime.combine(date, time(8,0)),
                                    'Install',
                                    notes,
                                    'scheduled'
                                ]).collect()
                                
                                if secondary_tech:
                                    session.sql("""
                                        INSERT INTO APPOINTMENTS (
                                            APPOINTMENTID, CUSTOMERID, TECHNICIANID,
                                            SCHEDULED_TIME, SERVICE_TYPE, NOTES, STA_TUS
                                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                    """, params=[
                                        f'APT{datetime.now().timestamp()}',
                                        st.session_state['selected_customer_id'],
                                        secondary_tech,
                                        datetime.combine(date, time(8,0)),
                                        'Install-Assist',
                                        notes,
                                        'scheduled'
                                    ]).collect()
                                
                                st.success(f"Installation booked for {date.strftime('%A, %B %d')}!")
                                del st.session_state.selected_install_date
                                del st.session_state['selected_customer_id']
                                del st.session_state['selected_customer_name']
                                st.rerun()
                            
                            except Exception as e:
                                st.error(f"Error: {str(e)}")
                
                else:
                    st.subheader("Select Appointment Time")
                    
                    today = datetime.now().date()
                    if 'week_offset' not in st.session_state:
                        st.session_state.week_offset = 0
                    
                    col1, col2, col3 = st.columns([2,1,1])
                    with col1:
                        st.write(f"Week of {(today + timedelta(weeks=st.session_state.week_offset)).strftime('%B %d')}")
                    with col2:
                        if st.button("◀ Previous Week"):
                            st.session_state.week_offset -= 1
                            st.rerun()
                    with col3:
                        if st.button("Next Week ▶"):
                            st.session_state.week_offset += 1
                            st.rerun()
                    
                    start_date = today + timedelta(weeks=st.session_state.week_offset) - timedelta(days=today.weekday())
                    days = [start_date + timedelta(days=i) for i in range(7)]
                    
                    appointments = session.sql("""
                        SELECT * FROM APPOINTMENTS
                        WHERE DATE(SCHEDULED_TIME) BETWEEN ? AND ?
                        AND STA_TUS != 'cancelled'
                    """, params=[start_date, start_date + timedelta(days=6)]).collect()
                    
                    time_slots = [time(hour) for hour in range(8, 19, 2)]
                    
                    for day in days:
                        with st.expander(day.strftime("%A %m/%d"), expanded=True):
                            cols = st.columns(len(time_slots))
                            
                            for i, time_slot in enumerate(time_slots):
                                slot_start = datetime.combine(day, time_slot)
                                slot_end = slot_start + timedelta(hours=2)
                                
                                with cols[i]:
                                    available_techs = []
                                    for tech in technicians:
                                        tech_id = tech['EMPLOYEEID']
                                        
                                        is_busy = any(
                                            a for a in appointments 
                                            if a['TECHNICIANID'] == tech_id
                                            and datetime.combine(day, a['SCHEDULED_TIME'].time()) < slot_end
                                            and (datetime.combine(day, a['SCHEDULED_TIME'].time()) + timedelta(hours=2)) > slot_start
                                        )
                                        
                                        if not is_busy:
                                            available_techs.append(tech)
                                    
                                    slot_label = f"{time_slot.hour}-{(time_slot.hour+2)%12 or 12}"
                                    
                                    if available_techs:
                                        if st.button(
                                            slot_label,
                                            key=f"slot_{day}_{time_slot}",
                                            help="Available: " + ", ".join([t['ENAME'].split()[0] for t in available_techs])
                                        ):
                                            st.session_state.selected_slot = {
                                                'datetime': slot_start,
                                                'techs': available_techs
                                            }
                                            st.rerun()
                                    else:
                                        st.button(
                                            slot_label,
                                            disabled=True,
                                            key=f"slot_{day}_{time_slot}_disabled"
                                        )
                    
                    if 'selected_slot' in st.session_state:
                        slot = st.session_state.selected_slot
                        time_range = f"{slot['datetime'].hour}-{slot['datetime'].hour+2}"
                        st.success(f"Selected: {slot['datetime'].strftime('%A %m/%d')} {time_range}")
                        
                        primary_tech = st.selectbox(
                            "Primary Technician",
                            options=[t['EMPLOYEEID'] for t in slot['techs']],
                            format_func=lambda x: next(t['ENAME'] for t in slot['techs'] if t['EMPLOYEEID'] == x)
                        )
                        
                        secondary_techs = [t for t in slot['techs'] if t['EMPLOYEEID'] != primary_tech]
                        secondary_tech = st.selectbox(
                            "Additional Technician (Optional)",
                            options=[""] + [t['EMPLOYEEID'] for t in secondary_techs],
                            format_func=lambda x: next(t['ENAME'] for t in slot['techs'] if t['EMPLOYEEID'] == x) if x else "None"
                        )
                        
                        notes = st.text_area("Service Notes")
                        if is_urgent == "Yes":
                            notes = "URGENT: " + notes
                        
                        if st.button("Book Appointment"):
                            try:
                                existing = session.sql("""
                                    SELECT * FROM APPOINTMENTS
                                    WHERE TECHNICIANID = ?
                                    AND DATE(SCHEDULED_TIME) = ?
                                    AND HOUR(SCHEDULED_TIME) = ?
                                    AND STA_TUS != 'cancelled'
                                """, params=[primary_tech, slot['datetime'].date(), slot['datetime'].hour]).collect()
                                
                                if existing:
                                    st.error("Time slot no longer available")
                                    del st.session_state.selected_slot
                                    st.rerun()
                                
                                session.sql("""
                                    INSERT INTO APPOINTMENTS (
                                        APPOINTMENTID, CUSTOMERID, TECHNICIANID, 
                                        SCHEDULED_TIME, SERVICE_TYPE, NOTES, STA_TUS
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                """, params=[
                                    f'APT{datetime.now().timestamp()}',
                                    st.session_state['selected_customer_id'],
                                    primary_tech,
                                    slot['datetime'],
                                    request_type,
                                    notes,
                                    'scheduled'
                                ]).collect()
                                
                                if secondary_tech:
                                    session.sql("""
                                        INSERT INTO APPOINTMENTS (
                                            APPOINTMENTID, CUSTOMERID, TECHNICIANID, 
                                            SCHEDULED_TIME, SERVICE_TYPE, NOTES, STA_TUS
                                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                    """, params=[
                                        f'APT{datetime.now().timestamp()}',
                                        st.session_state['selected_customer_id'],
                                        secondary_tech,
                                        slot['datetime'],
                                        f"{request_type}-Assist",
                                        notes,
                                        'scheduled'
                                    ]).collect()
                                
                                st.success(f"Appointment booked for {time_range}!")
                                del st.session_state.selected_slot
                                del st.session_state['selected_customer_id']
                                del st.session_state['selected_customer_name']
                                st.rerun()
                            
                            except Exception as e:
                                st.error(f"Error: {str(e)}")
            
            if st.button("Back to Customer List"):
                if 'selected_install_date' in st.session_state:
                    del st.session_state.selected_install_date
                if 'selected_slot' in st.session_state:
                    del st.session_state.selected_slot
                del st.session_state['selected_customer_id']
                del st.session_state['selected_customer_name']
                st.rerun()

        if 'edit_customer' in st.session_state and 'customer_to_edit' in st.session_state:
            edit_customer_id = st.session_state['edit_customer']
            customer_to_edit = st.session_state['customer_to_edit']
            
            st.subheader("✏️ Edit Customer")
            with st.form(key="edit_customer_form"):
                col1, col2 = st.columns(2)
                
                with col1:
                    name = st.text_input("Full Name*", value=customer_to_edit['NAME'])
                    phone = st.text_input("Phone*", value=customer_to_edit['PHONE'])
                    email = st.text_input("Email", value=customer_to_edit.get('EMAIL', ''))
                    address = st.text_input("Street Address*", value=customer_to_edit['ADDRESS'])
                    unit = st.text_input("Unit/Apt", value=customer_to_edit.get('UNIT', ''))
                    
                with col2:
                    city = st.text_input("City*", value=customer_to_edit['CITY'])
                    state = st.selectbox("State*", ["MD", "DC", "VA"], 
                                       index=["MD", "DC", "VA"].index(customer_to_edit['STATE']))
                    zipcode = st.text_input("Zip Code*", value=customer_to_edit['ZIPCODE'])
                    
                    how_heard_value = customer_to_edit.get('HOW_HEARD', '')
                    if isinstance(how_heard_value, str) and ":" in how_heard_value:
                        how_heard = how_heard_value.split(":")[0].strip()
                        friend_name = how_heard_value.split(":")[1].strip() if how_heard == "Friend" else ""
                    else:
                        how_heard = how_heard_value
                        friend_name = ""
                    
                    how_heard = st.selectbox(
                        "How did you hear about us?",
                        ["", "Google", "Friend", "Facebook", "Yelp", "Other"],
                        index=["", "Google", "Friend", "Facebook", "Yelp", "Other"].index(how_heard) if how_heard in ["", "Google", "Friend", "Facebook", "Yelp", "Other"] else 0
                    )
                    
                    if how_heard == "Friend":
                        friend_name = st.text_input("Friend's Name*", value=friend_name)
                
                col1, col2 = st.columns(2)
                with col1:
                    has_lock_box = st.radio(
                        "Lock Box", 
                        ["No", "Yes"], 
                        index=1 if customer_to_edit.get('HAS_LOCK_BOX') == 'Yes' else 0,
                        horizontal=True
                    )
                    lock_box_code = ""
                    if has_lock_box == "Yes":
                        lock_box_code = st.text_input("Lock Box Code*", value=customer_to_edit.get('LOCK_BOX_CODE', ''))
                
                with col2:
                    has_safety_alarm = st.radio(
                        "Safety Alarm", 
                        ["No", "Yes"], 
                        index=1 if customer_to_edit.get('HAS_SAFETY_ALARM') == 'Yes' else 0,
                        horizontal=True
                    )
                    safety_alarm_code = ""
                    if has_safety_alarm == "Yes":
                        safety_alarm_code = st.text_input("Safety Alarm Code*", value=customer_to_edit.get('SAFETY_ALARM', ''))
                
                st.subheader("Equipment Information")
                col1, col2 = st.columns(2)
                with col1:
                    outdoor_unit_model = st.text_input("Outdoor Unit Model", value=customer_to_edit.get('OUTDOOR_UNIT_MODEL', ''))
                    outdoor_unit_serial = st.text_input("Outdoor Unit Serial Number", value=customer_to_edit.get('OUTDOOR_UNIT_SERIAL_NUMBER', ''))
                    indoor_unit_model = st.text_input("Indoor Unit Model", value=customer_to_edit.get('INDOOR_UNIT_MODEL', ''))
                    indoor_unit_serial = st.text_input("Indoor Unit Serial Number", value=customer_to_edit.get('INDOOR_UNIT_SERIAL_NUMBER', ''))
                    
                with col2:
                    thermostat_type = st.text_input("Thermostat Type", value=customer_to_edit.get('THERMOSTAT_TYPE', ''))
                    unit_location = st.selectbox(
                        "Unit Location",
                        ["Attic", "Basement", "Garage", "Closet", "Crawlspace", "Other"],
                        index=["Attic", "Basement", "Garage", "Closet", "Crawlspace", "Other"].index(
                            customer_to_edit.get('UNIT_LOCATION', 'Attic'))
                    )
                    accessibility_level = st.selectbox(
                        "Accessibility Level",
                        ["Easy", "Moderate", "Difficult", "Very Difficult"],
                        index=["Easy", "Moderate", "Difficult", "Very Difficult"].index(
                            customer_to_edit.get('ACCESSIBILITY_LEVEL', 'Easy'))
                    )
                
                note = st.text_area("General Notes", value=customer_to_edit.get('NOTE', ''))
                entrance_note = st.text_area("Entrance Notes", value=customer_to_edit.get('ENTRANCE_NOTE', ''))
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.form_submit_button("💾 Save Changes"):
                        if not all([name, phone, address, city, state, zipcode]):
                            st.error("Please fill in all required fields (*)")
                        elif not re.match(r"^\d{3}-\d{3}-\d{4}$", phone):
                            st.error("Invalid phone number format. Please use ###-###-####")
                        elif has_lock_box == "Yes" and not lock_box_code:
                            st.error("Please enter Lock Box Code when Lock Box is set to Yes")
                        elif has_safety_alarm == "Yes" and not safety_alarm_code:
                            st.error("Please enter Safety Alarm Code when Safety Alarm is set to Yes")
                        elif how_heard == "Friend" and not friend_name:
                            st.error("Please enter Friend's Name when referral is from a friend")
                        else:
                            try:
                                how_heard_value = how_heard
                                if how_heard == "Friend":
                                    how_heard_value = f"Friend: {friend_name}"
                                
                                session.sql("""
                                    UPDATE CUSTOMERS 
                                    SET NAME = ?,
                                        PHONE = ?,
                                        EMAIL = ?,
                                        ADDRESS = ?,
                                        UNIT = ?,
                                        CITY = ?,
                                        STATE = ?,
                                        ZIPCODE = ?,
                                        HAS_LOCK_BOX = ?,
                                        LOCK_BOX_CODE = ?,
                                        HAS_SAFETY_ALARM = ?,
                                        SAFETY_ALARM = ?,
                                        HOW_HEARD = ?,
                                        NOTE = ?,
                                        ENTRANCE_NOTE = ?,
                                        OUTDOOR_UNIT_MODEL = ?,
                                        OUTDOOR_UNIT_SERIAL_NUMBER = ?,
                                        INDOOR_UNIT_MODEL = ?,
                                        INDOOR_UNIT_SERIAL_NUMBER = ?,
                                        THERMOSTAT_TYPE = ?,
                                        UNIT_LOCATION = ?,
                                        ACCESSIBILITY_LEVEL = ?
                                    WHERE CUSTOMERID = ?
                                """, params=[
                                    name,
                                    phone,
                                    email,
                                    address,
                                    unit,
                                    city,
                                    state,
                                    zipcode,
                                    has_lock_box,
                                    lock_box_code if has_lock_box == "Yes" and lock_box_code else None,
                                    has_safety_alarm,
                                    safety_alarm_code if has_safety_alarm == "Yes" and safety_alarm_code else None,
                                    how_heard_value,
                                    note,
                                    entrance_note,
                                    outdoor_unit_model,
                                    outdoor_unit_serial,
                                    indoor_unit_model,
                                    indoor_unit_serial,
                                    thermostat_type,
                                    unit_location,
                                    accessibility_level,
                                    edit_customer_id
                                ]).collect()
                                
                                st.success("Customer updated successfully!")
                                del st.session_state['edit_customer']
                                del st.session_state['customer_to_edit']
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error updating customer: {str(e)}")
                
                with col2:
                    if st.form_submit_button("❌ Cancel"):
                        del st.session_state['edit_customer']
                        del st.session_state['customer_to_edit']
                        st.rerun()
                        
    except Exception as e:
        st.error(f"Customer management error: {str(e)}")
        st.error(traceback.format_exc())

def appointments():
    try:
        session = get_session()
        st.subheader("📅 Appointment Scheduling")
        
        st.subheader("1. Select Customer")
        search_query = st.text_input("Search by Name, Phone, Email, or Address", key="customer_search")
        
        customers = session.sql("""
            SELECT CUSTOMERID, NAME, PHONE FROM CUSTOMERS 
            WHERE NAME ILIKE ?
            ORDER BY NAME
        """, params=[f"%{search_query}%"]).collect()
        
        if not customers:
            st.warning("No customers found")
            return
        
        selected_customer_id = st.selectbox(
            "Select Customer",
            options=[row['CUSTOMERID'] for row in customers],
            format_func=lambda x: next(f"{row['NAME']} ({row['PHONE']})" for row in customers if row['CUSTOMERID'] == x)
        )

        st.subheader("2. Service Request")
        request_type = st.selectbox(
            "Select Request Type",
            ["Install", "Service", "Estimate"],
            index=0
        )

        expertise_map = {"Install": "EX1", "Service": "EX2", "Estimate": "EX3"}
        technicians = session.sql("""
            SELECT e.EMPLOYEEID, e.ENAME 
            FROM EMPLOYEES e
            JOIN EMPLOYEE_EXPERTISE ee ON e.EMPLOYEEID = ee.EMPLOYEEID
            WHERE ee.EXPERTISEID = ?
        """, params=[expertise_map[request_type]]).collect()
        
        if not technicians:
            st.error("No technicians available")
            return

        if request_type == "Install":
            st.subheader("3. Select Installation Date")
            
            start_date = datetime.now().date()
            dates = [start_date + timedelta(days=i) for i in range(28)]
            
            booked_days = session.sql("""
                SELECT DISTINCT DATE(SCHEDULED_TIME) as DAY 
                FROM APPOINTMENTS 
                WHERE SERVICE_TYPE = 'Install'
                AND DATE(SCHEDULED_TIME) BETWEEN ? AND ?
            """, params=[start_date, start_date + timedelta(days=28)]).collect()
            booked_days = [row['DAY'] for row in booked_days]
            
            cols = st.columns(7)
            for i, date in enumerate(dates):
                with cols[i % 7]:
                    if date in booked_days:
                        st.button(
                            f"{date.strftime('%a %m/%d')}",
                            disabled=True,
                            key=f"install_day_{date}"
                        )
                    else:
                        if st.button(
                            f"{date.strftime('%a %m/%d')}",
                            key=f"install_day_{date}"
                        ):
                            st.session_state.selected_install_date = date
            
            if 'selected_install_date' in st.session_state:
                date = st.session_state.selected_install_date
                st.success(f"Selected installation date: {date.strftime('%A, %B %d')}")
                
                primary_tech = st.selectbox(
                    "Primary Technician",
                    options=[t['EMPLOYEEID'] for t in technicians],
                    format_func=lambda x: next(t['ENAME'] for t in technicians if t['EMPLOYEEID'] == x)
                )
                
                secondary_techs = [t for t in technicians if t['EMPLOYEEID'] != primary_tech]
                secondary_tech = st.selectbox(
                    "Additional Technician (Optional)",
                    options=[""] + [t['EMPLOYEEID'] for t in secondary_techs],
                    format_func=lambda x: next(t['ENAME'] for t in technicians if t['EMPLOYEEID'] == x) if x else "None"
                )
                
                notes = st.text_area("Installation Notes")
                
                if st.button("Book Installation"):
                    try:
                        session.sql("""
                            INSERT INTO APPOINTMENTS (
                                APPOINTMENTID, CUSTOMERID, TECHNICIANID,
                                SCHEDULED_TIME, SERVICE_TYPE, NOTES, STA_TUS
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, params=[
                            f'APT{datetime.now().timestamp()}',
                            selected_customer_id,
                            primary_tech,
                            datetime.combine(date, time(8,0)),
                            'Install',
                            notes,
                            'scheduled'
                        ]).collect()
                        
                        if secondary_tech:
                            session.sql("""
                                INSERT INTO APPOINTMENTS (
                                    APPOINTMENTID, CUSTOMERID, TECHNICIANID,
                                    SCHEDULED_TIME, SERVICE_TYPE, NOTES, STA_TUS
                                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, params=[
                                f'APT{datetime.now().timestamp()}',
                                selected_customer_id,
                                secondary_tech,
                                datetime.combine(date, time(8,0)),
                                'Install-Assist',
                                notes,
                                'scheduled'
                            ]).collect()
                        
                        st.success(f"Installation booked for {date.strftime('%A, %B %d')}!")
                        del st.session_state.selected_install_date
                        st.rerun()
                    
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

        else:
            st.subheader("3. Select Appointment Time")
            
            today = datetime.now().date()
            if 'week_offset' not in st.session_state:
                st.session_state.week_offset = 0
            
            col1, col2, col3 = st.columns([2,1,1])
            with col1:
                st.write(f"Week of {(today + timedelta(weeks=st.session_state.week_offset)).strftime('%B %d')}")
            with col2:
                if st.button("◀ Previous Week"):
                    st.session_state.week_offset -= 1
                    st.rerun()
            with col3:
                if st.button("Next Week ▶"):
                    st.session_state.week_offset += 1
                    st.rerun()
            
            start_date = today + timedelta(weeks=st.session_state.week_offset) - timedelta(days=today.weekday())
            days = [start_date + timedelta(days=i) for i in range(7)]
            
            appointments = session.sql("""
                SELECT * FROM APPOINTMENTS
                WHERE DATE(SCHEDULED_TIME) BETWEEN ? AND ?
                AND STA_TUS != 'cancelled'
            """, params=[start_date, start_date + timedelta(days=6)]).collect()
            
            time_slots = [time(hour) for hour in range(8, 19, 2)]
            
            for day in days:
                with st.expander(day.strftime("%A %m/%d"), expanded=True):
                    cols = st.columns(len(time_slots))
                    
                    for i, time_slot in enumerate(time_slots):
                        slot_start = datetime.combine(day, time_slot)
                        slot_end = slot_start + timedelta(hours=2)
                        
                        with cols[i]:
                            available_techs = []
                            for tech in technicians:
                                tech_id = tech['EMPLOYEEID']
                                
                                is_busy = any(
                                    a for a in appointments 
                                    if a['TECHNICIANID'] == tech_id
                                    and datetime.combine(day, a['SCHEDULED_TIME'].time()) < slot_end
                                    and (datetime.combine(day, a['SCHEDULED_TIME'].time()) + timedelta(hours=2)) > slot_start
                                )
                                
                                if not is_busy:
                                    available_techs.append(tech)
                            
                            slot_label = f"{time_slot.hour}-{(time_slot.hour+2)%12 or 12}"
                            
                            if available_techs:
                                if st.button(
                                    slot_label,
                                    key=f"slot_{day}_{time_slot}",
                                    help="Available: " + ", ".join([t['ENAME'].split()[0] for t in available_techs])
                                ):
                                    st.session_state.selected_slot = {
                                        'datetime': slot_start,
                                        'techs': available_techs
                                    }
                                    st.rerun()
                            else:
                                st.button(
                                    slot_label,
                                    disabled=True,
                                    key=f"slot_{day}_{time_slot}_disabled"
                                )
            
            if 'selected_slot' in st.session_state:
                slot = st.session_state.selected_slot
                time_range = f"{slot['datetime'].hour}-{slot['datetime'].hour+2}"
                st.success(f"Selected: {slot['datetime'].strftime('%A %m/%d')} {time_range}")
                
                primary_tech = st.selectbox(
                    "Primary Technician",
                    options=[t['EMPLOYEEID'] for t in slot['techs']],
                    format_func=lambda x: next(t['ENAME'] for t in slot['techs'] if t['EMPLOYEEID'] == x)
                )
                
                secondary_techs = [t for t in slot['techs'] if t['EMPLOYEEID'] != primary_tech]
                secondary_tech = st.selectbox(
                    "Additional Technician (Optional)",
                    options=[""] + [t['EMPLOYEEID'] for t in secondary_techs],
                    format_func=lambda x: next(t['ENAME'] for t in slot['techs'] if t['EMPLOYEEID'] == x) if x else "None"
                )
                
                notes = st.text_area("Service Notes")
                
                if st.button("Book Appointment"):
                    try:
                        existing = session.sql("""
                            SELECT * FROM APPOINTMENTS
                            WHERE TECHNICIANID = ?
                            AND DATE(SCHEDULED_TIME) = ?
                            AND HOUR(SCHEDULED_TIME) = ?
                            AND STA_TUS != 'cancelled'
                        """, params=[primary_tech, slot['datetime'].date(), slot['datetime'].hour]).collect()
                        
                        if existing:
                            st.error("Time slot no longer available")
                            del st.session_state.selected_slot
                            st.rerun()
                        
                        session.sql("""
                            INSERT INTO APPOINTMENTS (
                                APPOINTMENTID, CUSTOMERID, TECHNICIANID, 
                                SCHEDULED_TIME, SERVICE_TYPE, NOTES, STA_TUS
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, params=[
                            f'APT{datetime.now().timestamp()}',
                            selected_customer_id,
                            primary_tech,
                            slot['datetime'],
                            request_type,
                            notes,
                            'scheduled'
                        ]).collect()
                        
                        if secondary_tech:
                            session.sql("""
                                INSERT INTO APPOINTMENTS (
                                    APPOINTMENTID, CUSTOMERID, TECHNICIANID, 
                                    SCHEDULED_TIME, SERVICE_TYPE, NOTES, STA_TUS
                                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, params=[
                                f'APT{datetime.now().timestamp()}',
                                selected_customer_id,
                                secondary_tech,
                                slot['datetime'],
                                f"{request_type}-Assist",
                                notes,
                                'scheduled'
                            ]).collect()
                        
                        st.success(f"Appointment booked for {time_range}!")
                        del st.session_state.selected_slot
                        st.rerun()
                    
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

        st.subheader("Current Appointments This Week")
        current_appts = session.sql("""
            SELECT 
                a.APPOINTMENTID,
                c.NAME as CUSTOMER_NAME,
                e.ENAME as TECHNICIAN_NAME,
                a.SCHEDULED_TIME,
                a.SERVICE_TYPE,
                a.STA_TUS,
                a.NOTES
            FROM APPOINTMENTS a
            JOIN CUSTOMERS c ON a.CUSTOMERID = c.CUSTOMERID
            JOIN EMPLOYEES e ON a.TECHNICIANID = e.EMPLOYEEID
            WHERE DATE(a.SCHEDULED_TIME) BETWEEN ? AND ?
            ORDER BY a.SCHEDULED_TIME
        """, params=[start_date, start_date + timedelta(days=6)]).collect()
        
        if current_appts:
            appt_data = []
            for appt in current_appts:
                start = appt['SCHEDULED_TIME']
                time_range = f"{start.hour}-{start.hour+2}"
                
                appt_data.append({
                    "Date": start.strftime('%a %m/%d'),
                    "Time": time_range,
                    "Customer": appt['CUSTOMER_NAME'],
                    "Technician": appt['TECHNICIAN_NAME'],
                    "Service": appt['SERVICE_TYPE'],
                    "Status": appt['STA_TUS']
                })
            
            st.dataframe(
                pd.DataFrame(appt_data),
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info("No appointments scheduled for this week")
            
    except Exception as e:
        st.error(f"Appointments page error: {str(e)}")
        st.error(traceback.format_exc())

def admin_tables():
    try:
        session = get_session()
        st.subheader("🛠 Admin Tables")
        
        tables = [
            "EMPLOYEES", "CUSTOMERS", "APPOINTMENTS", 
            "ROLES", "EMPLOYEE_ROLES", "EXPERTISE", "EMPLOYEE_EXPERTISE",
            "EMPLOYEE_SCHEDULES"
        ]
        
        selected_table = st.selectbox("Select Table", tables)
        
        if selected_table == "EMPLOYEE_SCHEDULES":
            st.subheader("📅 Employee Schedule Management")
            
            today = datetime.now().date()
            start_of_week = today - timedelta(days=today.weekday())
            end_of_week = start_of_week + timedelta(days=6)
            
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Week Starting", value=start_of_week)
            with col2:
                end_date = st.date_input("Week Ending", value=end_of_week)
            
            employees = session.sql("SELECT EMPLOYEEID, ENAME FROM EMPLOYEES ORDER BY ENAME").collect()
            employee_options = {e['EMPLOYEEID']: e['ENAME'] for e in employees}
            
            schedules = session.sql("""
                SELECT s.*, e.ENAME 
                FROM EMPLOYEE_SCHEDULES s
                JOIN EMPLOYEES e ON s.EMPLOYEEID = e.EMPLOYEEID
                WHERE s.SCHEDULE_DATE BETWEEN ? AND ?
                ORDER BY s.SCHEDULE_DATE, s.START_TIME
            """, params=[start_date, end_date]).collect()
            
            st.subheader(f"📅 Weekly Schedule: {start_date.strftime('%m/%d')} - {end_date.strftime('%m/%d')}")
            
            time_slots = [
                ("8:00-10:00", time(8, 0), time(10, 0)),
                ("10:00-12:00", time(10, 0), time(12, 0)),
                ("12:00-14:00", time(12, 0), time(14, 0)),
                ("14:00-16:00", time(14, 0), time(16, 0)),
                ("16:00-18:00", time(16, 0), time(18, 0))
            ]
            
            days = [(start_date + timedelta(days=i)).strftime("%a %m/%d") for i in range(7)]
            day_dates = [start_date + timedelta(days=i) for i in range(7)]
            
            st.markdown("""
            <style>
                .employee-box {
                    display: inline-block;
                    background-color: #e6f7ff;
                    border-radius: 4px;
                    padding: 2px 6px;
                    margin: 2px;
                    font-size: 12px;
                    border: 1px solid #b3e0ff;
                }
                .schedule-table {
                    width: 100%;
                    border-collapse: collapse;
                }
                .schedule-table th, .schedule-table td {
                    border: 1px solid #ddd;
                    padding: 8px;
                    text-align: center;
                }
                .schedule-table th {
                    background-color: #f2f2f2;
                    font-weight: bold;
                }
                .time-col {
                    background-color: #f9f9f9;
                    font-weight: bold;
                }
            </style>
            """, unsafe_allow_html=True)
            
            table_html = """
            <table class="schedule-table">
                <tr>
                    <th>Time Slot</th>
            """
            for day in days:
                table_html += f"<th>{day}</th>"
            table_html += "</tr>"
            
            for slot_name, slot_start, slot_end in time_slots:
                table_html += f"<tr><td class='time-col'>{slot_name}</td>"
                
                for day_date in day_dates:
                    day_schedules = []
                    for s in schedules:
                        if s['SCHEDULE_DATE'] == day_date:
                            s_start = s['START_TIME']
                            s_end = s['END_TIME']
                            if (s_start < slot_end) and (s_end > slot_start):
                                day_schedules.append(s['ENAME'])
                    
                    table_html += "<td>"
                    for name in day_schedules:
                        table_html += f"<div class='employee-box'>{name}</div>"
                    table_html += "</td>"
                
                table_html += "</tr>"
            
            table_html += "</table>"
            st.markdown(table_html, unsafe_allow_html=True)
            
            with st.expander("✏️ Add New Schedule"):
                with st.form("schedule_form"):
                    col1, col2 = st.columns(2)
                    with col1:
                        employee = st.selectbox(
                            "Employee",
                            options=list(employee_options.keys()),
                            format_func=lambda x: employee_options[x]
                        )
                        schedule_date = st.date_input(
                            "Date",
                            min_value=start_date,
                            max_value=end_date
                        )
                    with col2:
                        start_time = st.time_input("Start Time", value=time(8, 0))
                        end_time = st.time_input("End Time", value=time(17, 0))
                    
                    notes = st.text_input("Notes (optional)")
                    
                    submitted = st.form_submit_button("Save Schedule")
                    if submitted:
                        if start_time >= end_time:
                            st.error("End time must be after start time!")
                        else:
                            existing = session.sql("""
                                SELECT * FROM EMPLOYEE_SCHEDULES
                                WHERE EMPLOYEEID = ?
                                AND SCHEDULE_DATE = ?
                                AND (
                                    (START_TIME < ? AND END_TIME > ?)
                                )
                            """, params=[employee, schedule_date, end_time, start_time]).collect()
                            
                            if existing:
                                st.error("This employee already has a schedule during this time period!")
                            else:
                                duplicate = session.sql("""
                                    SELECT * FROM EMPLOYEE_SCHEDULES
                                    WHERE EMPLOYEEID = ?
                                    AND SCHEDULE_DATE = ?
                                    AND START_TIME = ?
                                    AND END_TIME = ?
                                """, params=[employee, schedule_date, start_time, end_time]).collect()
                                
                                if duplicate:
                                    st.error("This exact schedule already exists for this employee!")
                                else:
                                    schedule_id = f"SCH{datetime.now().timestamp()}"
                                    session.sql("""
                                        INSERT INTO EMPLOYEE_SCHEDULES (
                                            SCHEDULEID, EMPLOYEEID, SCHEDULE_DATE, 
                                            START_TIME, END_TIME, NOTES
                                        ) VALUES (?, ?, ?, ?, ?, ?)
                                    """, params=[schedule_id, employee, schedule_date, start_time, end_time, notes]).collect()
                                    st.success("Schedule added successfully!")
                                    st.rerun()
            
            with st.expander("🗑️ Delete Schedules"):
                if schedules:
                    schedule_groups = {}
                    for s in schedules:
                        key = f"{s['ENAME']} - {s['SCHEDULE_DATE']}"
                        if key not in schedule_groups:
                            schedule_groups[key] = []
                        schedule_groups[key].append(s)
                    
                    selected_group = st.selectbox(
                        "Select employee and date",
                        options=list(schedule_groups.keys())
                    )
                    
                    if selected_group:
                        group_schedules = schedule_groups[selected_group]
                        selected_schedule = st.selectbox(
                            "Select schedule to delete",
                            options=[f"{s['START_TIME']} to {s['END_TIME']} ({s['NOTES'] or 'no notes'})" 
                                    for s in group_schedules],
                            key="delete_schedule_select"
                        )
                        
                        if st.button("Delete Selected Schedule"):
                            schedule_id = group_schedules[
                                [f"{s['START_TIME']} to {s['END_TIME']} ({s['NOTES'] or 'no notes'})" 
                                 for s in group_schedules].index(selected_schedule)
                            ]['SCHEDULEID']
                            session.sql("""
                                DELETE FROM EMPLOYEE_SCHEDULES
                                WHERE SCHEDULEID = ?
                            """, params=[schedule_id]).collect()
                            st.success("Schedule deleted!")
                            st.rerun()
                else:
                    st.info("No schedules to delete for selected week")
        
        else:
            st.subheader(f"Manage {selected_table.capitalize()}")
            
            table_data = session.table(selected_table).collect()
            if table_data:
                st.dataframe(table_data)
            
            with st.expander("➕ Add New Record"):
                with st.form(f"add_{selected_table}_form"):
                    columns = session.table(selected_table).columns
                    input_values = {}
                    for col in columns:
                        if col.lower().endswith("id"):
                            continue
                        input_values[col] = st.text_input(f"{col}")
                    
                    if st.form_submit_button("Add Record"):
                        try:
                            if "id" in [c.lower() for c in columns]:
                                input_values[columns[0]] = f"{selected_table.upper()}_{datetime.now().timestamp()}"
                            
                            columns_str = ", ".join([f'"{col}"' for col in input_values.keys()])
                            values_str = ", ".join([f"'{input_values[col]}'" for col in input_values.keys()])
                            session.sql(f"""
                                INSERT INTO {selected_table} 
                                ({columns_str})
                                VALUES ({values_str})
                            """).collect()
                            st.success("Record added successfully!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error adding record: {str(e)}")
            
            with st.expander("✏️ Edit/Delete Record"):
                if table_data:
                    columns = session.table(selected_table).columns
                    selected_record = st.selectbox(
                        f"Select Record to Edit/Delete",
                        options=[row[columns[0]] for row in table_data]
                    )
                    
                    if selected_record:
                        record_data = [row for row in table_data if row[columns[0]] == selected_record][0]
                        
                        with st.form(f"edit_{selected_table}_form"):
                            edit_values = {}
                            for col in columns:
                                if col.lower().endswith("id"):
                                    st.text_input(f"{col} (Read-Only)", value=record_data[col], disabled=True)
                                else:
                                    edit_values[col] = st.text_input(f"{col}", value=record_data[col])
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                if st.form_submit_button("Update Record"):
                                    try:
                                        set_clause = ", ".join([f'"{col}" = \'{edit_values[col]}\'' for col in edit_values.keys()])
                                        session.sql(f"""
                                            UPDATE {selected_table} 
                                            SET {set_clause}
                                            WHERE "{columns[0]}" = '{selected_record}'
                                        """).collect()
                                        st.success("Record updated successfully!")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Error updating record: {str(e)}")
                            with col2:
                                if st.form_submit_button("Delete Record"):
                                    try:
                                        session.sql(f"""
                                            DELETE FROM {selected_table} 
                                            WHERE "{columns[0]}" = '{selected_record}'
                                        """).collect()
                                        st.success("Record deleted successfully!")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Error deleting record: {str(e)}")
                                        
    except Exception as e:
        st.error(f"Admin tables error: {str(e)}")
        st.error(traceback.format_exc())

def main_app():
    st.sidebar.title(f"Welcome {st.session_state.user_name}")

    available_tabs = set()
    for role in st.session_state.roles:
        available_tabs.update(ROLE_ACCESS.get(role.lower(), []))
    available_tabs.add("profile")

    tab_order = ['Home', 'profile', 'customers', 'appointments', 'admin_tables']
    available_tabs = [tab for tab in tab_order if tab in available_tabs]

    selected_tab = st.sidebar.selectbox("Navigation", available_tabs)

    if selected_tab == 'Home':
        Home()
    elif selected_tab == 'profile':
        profile_page()    
    elif selected_tab == 'customers':
        customer_management()
    elif selected_tab == 'appointments':
        appointments()
    elif selected_tab == 'admin_tables':
        admin_tables()

    if st.sidebar.button("Logout"):
        st.session_state.clear()
        st.rerun()

if __name__ == '__main__':
    query_params = st.query_params
    if 'reset_token' in query_params:
        reset_password(query_params['reset_token'])
    elif not st.session_state.get('logged_in'):
        login_page()
    else:
        main_app()
