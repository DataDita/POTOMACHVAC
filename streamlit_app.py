import streamlit.components.v1 as components
import streamlit as st
import snowflake.snowpark as sp
from snowflake.snowpark import Session
from datetime import datetime, timedelta, time
import re
import uuid
import hashlib
from PIL import Image, ImageOps
import io
import base64
import pandas as pd

cnx=st.connection("snowflake")
session = cnx.session()
from snowflake.snowpark import Session



##########################################################################################
##########################################################################################
##########################################################################################

def crop_to_square(image_bytes):
    img = Image.open(io.BytesIO(image_bytes))
    return ImageOps.fit(img, (120, 120))  # Crop to square

def process_image(image_bytes, target_width):
    """Resize image while maintaining quality and aspect ratio"""
    img = Image.open(io.BytesIO(image_bytes))
    
    # Calculate proportional height
    width_percent = (target_width / float(img.size[0]))
    height = int((float(img.size[1]) * float(width_percent)))
    
    # High-quality resizing
    img = img.resize((target_width, height), Image.Resampling.LANCZOS)
    
    # Convert back to bytes
    buffer = io.BytesIO()
    img.save(buffer, format="JPEG", quality=95)  # 95% quality
    return buffer.getvalue()

##########################################################################################
##########################################################################################

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
##########################################################################################
##########################################################################################
# Login page

def login_page():
    st.title("POTOMAC HVAC")
    emp_id = st.text_input("Employee ID")
    password = st.text_input("Password", type='password')
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Login"):
            session = get_session()
            try:
                result = session.sql(f"""
                    SELECT e.*, r.rolename
                    FROM employees e
                    JOIN employee_roles er ON e.employeeid = er.employeeid
                    JOIN roles r ON er.roleid = r.roleid
                    WHERE e.employeeid = '{emp_id}' AND e.password = '{password}'
                """).collect()
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
    with col2:
        if st.button("Forgot Password?"):
            st.session_state['show_forgot_password'] = True

    # Show "Forgot Password" section if enabled
    if st.session_state.get('show_forgot_password'):
        forgot_password()

        # Add a "Back to Login" button only in the "Forgot Password" flow
        if st.button("Back to Login"):
            st.session_state['show_forgot_password'] = False
            st.rerun()
##########################################################################################
# Forgot password functionality
def forgot_password():
    st.subheader("🔒 Forgot Password")
    email = st.text_input("Enter your email address")
    if st.button("Send Reset Link"):
        session = get_session()
        try:
            employee = session.sql(f"""
                SELECT employeeid FROM employees
                WHERE email = '{email}'
            """).collect()
            if employee:
                employee_id = employee[0]['EMPLOYEEID']
                reset_token = str(uuid.uuid4())
                token_hash = hashlib.sha256(reset_token.encode()).hexdigest()
                expires_at = datetime.now() + timedelta(hours=1)
                session.sql(f"""
                    INSERT INTO password_resets
                    (resetid, employeeid, reset_token, expires_at)
                    VALUES (
                        '{str(uuid.uuid4())}',
                        '{employee_id}',
                        '{token_hash}',
                        '{expires_at}'
                    )
                """).collect()
                st.success("Password reset link sent to your email!")
            else:
                st.error("No account found with that email address")
        except Exception as e:
            st.error(f"Error processing request: {str(e)}")
##########################################################################################
# Reset password functionality
def reset_password(token):
    st.subheader("🔑 Reset Password")
    new_password = st.text_input("New Password", type='password')
    confirm_password = st.text_input("Confirm Password", type='password')
    if st.button("Reset Password"):
        if new_password == confirm_password:
            session = get_session()
            try:
                token_hash = hashlib.sha256(token.encode()).hexdigest()
                reset_record = session.sql(f"""
                    SELECT * FROM password_resets
                    WHERE reset_token = '{token_hash}'
                    AND used = FALSE
                    AND expires_at > CURRENT_TIMESTAMP()
                """).collect()
                if reset_record:
                    employee_id = reset_record[0]['EMPLOYEEID']
                    session.sql(f"""
                        UPDATE employees
                        SET password = '{new_password}'
                        WHERE employeeid = '{employee_id}'
                    """).collect()
                    session.sql(f"""
                        UPDATE password_resets
                        SET used = TRUE
                        WHERE resetid = '{reset_record[0]['RESETID']}'
                    """).collect()
                    st.success("Password reset successfully!")
                    st.session_state.clear()
                    st.rerun()
                else:
                    st.error("Invalid or expired reset token")
            except Exception as e:
                st.error(f"Error resetting password: {str(e)}")
        else:
            st.error("Passwords do not match")
##########################################################################################
##########################################################################################
##########################################################################################
##########################################################################################

# Home

def Home():
    session = get_session()
    
    # Initialize all variables
    selected_date = datetime.now().date()
    manual_time = False
    manual_clock_in = None
    manual_clock_out = None
    manual_break_start = None
    manual_break_end = None
    is_clocked_in = False
    is_on_break = False
    time_entry = []
    break_entry = []

    # --- UI Layout ---
    st.title("Home")
    
    # Header with profile
    col1, col2 = st.columns([4, 1])
    with col1:
        st.subheader(f"Welcome, {st.session_state.user_name}!")
        st.caption(f"Role: {', '.join(st.session_state.roles)}")

    
    
    with col2:
        # profile picture display
        try:
            pic_data = session.sql(f"""
                SELECT PICTURE_DATA_TEXT FROM EMPLOYEE_PICTURES
                WHERE EMPLOYEEID = '{st.session_state.user_id}'
                ORDER BY UPLOADED_AT DESC LIMIT 1
            """).collect()
            
            if pic_data and pic_data[0]['PICTURE_DATA_TEXT']:
                img = Image.open(io.BytesIO(base64.b64decode(pic_data[0]['PICTURE_DATA_TEXT'])))
                st.image(img, width=100)
            else:
                st.image(Image.new('RGB', (100,100), color='gray'), width=100)
        except Exception as e:
            st.error(f"Couldn't load profile picture: {str(e)}")

  
    
    # --- Time Tracking Section ---
    st.subheader("Time Tracking")
    manual_time = st.toggle("Manual Time Entry", help="Enable to enter times manually")
    
    # Create time options from 7 AM to 10 PM in 12-hour format
    time_options = []
    for hour in range(7, 23):  # 7 AM to 10 PM (22:00)
        for minute in [0, 15, 30, 45]:  # 15-minute intervals
            time_obj = time(hour, minute)
            time_options.append((time_obj.strftime("%I:%M %p"), time_obj))
    
    if manual_time:
        with st.form("manual_time_form"):
            st.warning("Manual Entry Mode - For correcting missed punches")
            
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
                        # Convert to datetime objects
                        clock_in_dt = datetime.combine(selected_date, manual_clock_in)
                        clock_out_dt = datetime.combine(selected_date, manual_clock_out) if manual_clock_out else None
                        
                        # Check for existing entry
                        existing = session.sql(f"""
                            SELECT ENTRYID FROM employee_time_entries
                            WHERE EMPLOYEEID = '{st.session_state.user_id}'
                            AND ENTRY_DATE = '{selected_date}'
                            LIMIT 1
                        """).collect()
                        
                        if existing:
                            # Update existing entry
                            session.sql(f"""
                                UPDATE employee_time_entries
                                SET CLOCK_IN = '{clock_in_dt}',
                                    CLOCK_OUT = {'NULL' if clock_out_dt is None else f"'{clock_out_dt}'"}
                                WHERE ENTRYID = '{existing[0]['ENTRYID']}'
                            """).collect()
                        else:
                            # Create new entry
                            entry_id = f"ENTRY{datetime.now().timestamp()}"
                            session.sql(f"""
                                INSERT INTO employee_time_entries
                                (ENTRYID, EMPLOYEEID, CLOCK_IN, CLOCK_OUT, ENTRY_DATE)
                                VALUES (
                                    '{entry_id}',
                                    '{st.session_state.user_id}',
                                    '{clock_in_dt}',
                                    {'NULL' if clock_out_dt is None else f"'{clock_out_dt}'"},
                                    '{selected_date}'
                                )
                            """).collect()
                        
                        st.success("Time entry saved successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error saving time entry: {str(e)}")
    

    
    else:  # Automatic time tracking
        # Get current status
        time_entry = session.sql(f"""
            SELECT * FROM employee_time_entries
            WHERE EMPLOYEEID = '{st.session_state.user_id}'
            AND ENTRY_DATE = '{selected_date}'
            ORDER BY CLOCK_IN DESC
            LIMIT 1
        """).collect()
        
        break_entry = session.sql(f"""
            SELECT * FROM employee_break_entries
            WHERE EMPLOYEEID = '{st.session_state.user_id}'
            AND ENTRY_DATE = '{selected_date}'
            ORDER BY BREAK_START DESC
            LIMIT 1
        """).collect()
        
        is_clocked_in = len(time_entry) > 0 and time_entry[0]['CLOCK_OUT'] is None
        is_on_break = len(break_entry) > 0 and break_entry[0]['BREAK_END'] is None
        
        # Time tracking buttons
        cols = st.columns(2)
        with cols[0]:
            if st.button("🟢 Clock In", disabled=is_clocked_in,
                        ):
                session.sql(f"""
                    INSERT INTO employee_time_entries
                    (ENTRYID, EMPLOYEEID, CLOCK_IN, ENTRY_DATE)
                    VALUES (
                        'ENTRY{datetime.now().timestamp()}',
                        '{st.session_state.user_id}',
                        CURRENT_TIMESTAMP(),
                        '{selected_date}'
                    )
                """).collect()
                st.rerun()
        
        with cols[1]:
            if st.button("🔴 Clock Out", disabled=not is_clocked_in or is_on_break):
                session.sql(f"""
                    UPDATE employee_time_entries
                    SET CLOCK_OUT = CURRENT_TIMESTAMP()
                    WHERE EMPLOYEEID = '{st.session_state.user_id}'
                    AND ENTRY_DATE = '{selected_date}'
                    AND CLOCK_OUT IS NULL
                """).collect()
                st.rerun()
        
        # Break management buttons
        cols = st.columns(2)
        with cols[0]:
            if st.button("🟡 Start Break", disabled=not is_clocked_in or is_on_break):
                session.sql(f"""
                    INSERT INTO employee_break_entries
                    (BREAKID, EMPLOYEEID, BREAK_START, ENTRY_DATE)
                    VALUES (
                        'BREAK{datetime.now().timestamp()}',
                        '{st.session_state.user_id}',
                        CURRENT_TIMESTAMP(),
                        '{selected_date}'
                    )
                """).collect()
                st.rerun()
        
        with cols[1]:
            if st.button("🟢 End Break", disabled=not is_on_break):
                session.sql(f"""
                    UPDATE employee_break_entries
                    SET BREAK_END = CURRENT_TIMESTAMP()
                    WHERE EMPLOYEEID = '{st.session_state.user_id}'
                    AND ENTRY_DATE = '{selected_date}'
                    AND BREAK_END IS NULL
                """).collect()
                st.rerun()


   
    
    # Get latest status after potential updates
    time_entry = session.sql(f"""
        SELECT * FROM employee_time_entries
        WHERE EMPLOYEEID = '{st.session_state.user_id}'
        AND ENTRY_DATE = '{selected_date}'
        ORDER BY CLOCK_IN DESC
        LIMIT 1
    """).collect()
    
    break_entry = session.sql(f"""
        SELECT * FROM employee_break_entries
        WHERE EMPLOYEEID = '{st.session_state.user_id}'
        AND ENTRY_DATE = '{selected_date}'
        ORDER BY BREAK_START DESC
        LIMIT 1
    """).collect()
    
    cols = st.columns(2)
    with cols[0]:
        # Clock status
        if time_entry:
            if time_entry[0]['CLOCK_OUT'] is None:
                st.write(f"**Clocked In:**  {time_entry[0]['CLOCK_IN'].strftime('%I:%M %p')}")
            else:
                st.info("🔴 Clocked Out")
                st.write(f"**Worked:** {time_entry[0]['CLOCK_IN'].strftime('%I:%M %p')} to {time_entry[0]['CLOCK_OUT'].strftime('%I:%M %p')}")
       


        # Break status - only show if clocked in
        if time_entry and time_entry[0]['CLOCK_OUT'] is None:  # Only show break status if clocked in
            if break_entry:
                if break_entry[0]['BREAK_END'] is None:
                    st.error("🟡 Currently On Break")
                    st.write(f"**Since:** {break_entry[0]['BREAK_START'].strftime('%I:%M %p')}")
                else:
                    st.write(f"**Break:** {break_entry[0]['BREAK_START'].strftime('%I:%M %p')} to {break_entry[0]['BREAK_END'].strftime('%I:%M %p')}")
            else:
                st.success("✅ Available for Break")
    
    
    

    
    with col1:
        time_entries = session.sql(f"""
        SELECT CLOCK_IN, CLOCK_OUT 
        FROM employee_time_entries
        WHERE EMPLOYEEID = '{st.session_state.user_id}'
        AND ENTRY_DATE = '{selected_date}'
        ORDER BY CLOCK_IN
    """).collect()
    
    # Get all break entries for today
    break_entries = session.sql(f"""
        SELECT BREAK_START, BREAK_END 
        FROM employee_break_entries
        WHERE EMPLOYEEID = '{st.session_state.user_id}'
        AND ENTRY_DATE = '{selected_date}'
        ORDER BY BREAK_START
    """).collect()

    if time_entries:
        # Calculate total worked time (sum of all clock_in to clock_out periods)
        total_seconds = 0
        for entry in time_entries:
            if entry['CLOCK_OUT']:
                total_seconds += (entry['CLOCK_OUT'] - entry['CLOCK_IN']).total_seconds()
            else:
                total_seconds += (datetime.now() - entry['CLOCK_IN']).total_seconds()
        
        # Calculate total break time
        break_seconds = 0
        for entry in break_entries:
            if entry['BREAK_END']:
                break_seconds += (entry['BREAK_END'] - entry['BREAK_START']).total_seconds()
        
        # Calculate net worked time
        net_seconds = total_seconds - break_seconds
        hours = int(net_seconds // 3600)
        minutes = int((net_seconds % 3600) // 60)
        
        # Display in "X hours Y minutes" format
        time_str = f"{hours} hour{'s' if hours != 1 else ''} {minutes} minute{'s' if minutes != 1 else ''}"
        st.metric("Total Worked Today", time_str)
    

    # Appointments section
    with st.container(border=True):
        st.subheader("📅 Your Appointments")
    
    # Get all appointments (today + upcoming 7 days)
    appointments = session.sql(f"""
        SELECT 
            a.appointmentid,
            c.name AS customer_name,
            c.phone AS customer_phone,
            c.address AS customer_address,
            a.scheduled_time,
            TO_VARCHAR(a.scheduled_time, 'YYYY-MM-DD') AS appointment_date,
            DAYNAME(a.scheduled_time) AS day_of_week,
            TO_VARCHAR(a.scheduled_time, 'HH12:MI AM') AS time_formatted,
            a.sta_tus AS appointment_status,
            CASE 
                WHEN DATE(a.scheduled_time) = CURRENT_DATE() THEN 'Today'
                WHEN DATE(a.scheduled_time) = DATEADD('day', 1, CURRENT_DATE()) THEN 'Tomorrow'
                ELSE TO_VARCHAR(a.scheduled_time, 'Mon DD')
            END AS display_date
        FROM appointments a
        JOIN customers c ON a.customerid = c.customerid
        WHERE a.technicianid = '{st.session_state.user_id}'
        AND DATE(a.scheduled_time) BETWEEN CURRENT_DATE() AND DATEADD('day', 7, CURRENT_DATE())
        ORDER BY a.scheduled_time
    """).collect()
    
    if not appointments:
        st.info("No appointments scheduled for the next 7 days")
    else:
        # Display appointments with date, day, and time
        for appt in appointments:
            with st.expander(f"{appt['DAY_OF_WEEK']}, {appt['APPOINTMENT_DATE']} - {appt['CUSTOMER_NAME']} ({appt['TIME_FORMATTED']})"):
                st.write(f"**Date:** {appt['APPOINTMENT_DATE']} ({appt['DAY_OF_WEEK']})")
                st.write(f"**Time:** {appt['TIME_FORMATTED']}")
                st.write(f"**Customer:** {appt['CUSTOMER_NAME']}")
                st.write(f"**Phone:** {appt['CUSTOMER_PHONE']}")
                st.write(f"**Address:** {appt['CUSTOMER_ADDRESS']}")
                st.write(f"**Status:** {appt['APPOINTMENT_STATUS'].capitalize()}")

   
 
##########################################################################################
##########################################################################################
#######################################################################
##########################################################################################

#profile
def profile_page():
    session = get_session()
    st.title("User Profile")
    
    # --- 1. Profile Picture Section ---
    with st.container():
        col1, col2 = st.columns([1, 3])
        with col1:
            # Current profile picture display
            pic_data = session.sql(f"""
                SELECT PICTURE_DATA_TEXT FROM EMPLOYEE_PICTURES
                WHERE EMPLOYEEID = '{st.session_state.user_id}'
                ORDER BY UPLOADED_AT DESC LIMIT 1
            """).collect()
            
            if pic_data and pic_data[0]['PICTURE_DATA_TEXT']:
                img_data = base64.b64decode(pic_data[0]['PICTURE_DATA_TEXT'])
                img = Image.open(io.BytesIO(img_data))
                img.thumbnail((150, 150), Image.Resampling.LANCZOS)
                st.image(img, width=120)
            else:
                st.image(Image.new('RGB', (120, 120), color='lightgray'))
            
            # Picture upload
            uploaded_file = st.file_uploader("Update Profile Picture", type=["jpg", "jpeg", "png"])
            if uploaded_file and st.button("Update Picture"):
                try:
                    img = Image.open(uploaded_file)
                    img = ImageOps.fit(img, (500, 500))
                    buffer = io.BytesIO()
                    img.save(buffer, format="JPEG", quality=90)
                    encoded_image = base64.b64encode(buffer.getvalue()).decode('utf-8')
                    
                    session.sql(f"""
                        INSERT INTO EMPLOYEE_PICTURES 
                        (PICTUREID, EMPLOYEEID, PICTURE_DATA_TEXT)
                        VALUES (
                            'PIC{datetime.now().timestamp()}',
                            '{st.session_state.user_id}',
                            '{encoded_image}'
                        )
                    """).collect()
                    st.success("Picture updated!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {str(e)}")

    # --- 2. Weekly Schedule (Same as Admin View) ---
    with st.container():
        st.subheader("📅 Your Weekly Schedule")
        
        # Date range selection - default to current week
        today = datetime.now().date()
        start_of_week = today - timedelta(days=today.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        
        # Get employee name
        employee_name = session.sql(f"""
            SELECT ename FROM employees
            WHERE employeeid = '{st.session_state.user_id}'
        """).collect()[0]['ENAME']
        
        # Get all schedules for the current employee this week
        schedules = session.sql(f"""
            SELECT * FROM employee_schedules
            WHERE employeeid = '{st.session_state.user_id}'
            AND schedule_date BETWEEN '{start_of_week}' AND '{end_of_week}'
            ORDER BY schedule_date, start_time
        """).collect()
        
        # Create calendar view - days as columns, hours as rows
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
        
        # Define time slots (8AM to 6PM in 2-hour increments)
        time_slots = [
            ("8:00-10:00", time(8, 0), time(10, 0)),
            ("10:00-12:00", time(10, 0), time(12, 0)),
            ("12:00-14:00", time(12, 0), time(14, 0)),
            ("14:00-16:00", time(14, 0), time(16, 0)),
            ("16:00-18:00", time(16, 0), time(18, 0))
        ]
        
        # Get all days in the week
        days = [(start_of_week + timedelta(days=i)).strftime("%a %m/%d") for i in range(7)]
        day_dates = [start_of_week + timedelta(days=i) for i in range(7)]
        
        # Create HTML table
        table_html = """
        <table class="schedule-table">
            <tr>
                <th>Time Slot</th>
        """
        
        # Add day headers
        for day in days:
            table_html += f"<th>{day}</th>"
        table_html += "</tr>"
        
        # Add time slots and employee boxes
        for slot_name, slot_start, slot_end in time_slots:
            table_html += f"<tr><td class='time-col'>{slot_name}</td>"
            
            for day_date in day_dates:
                # Find schedules for this day and time slot
                scheduled = False
                for s in schedules:
                    if s['SCHEDULE_DATE'] == day_date:
                        s_start = s['START_TIME']
                        s_end = s['END_TIME']
                        # Check if schedule overlaps with time slot
                        if (s_start < slot_end) and (s_end > slot_start):
                            scheduled = True
                            break
                
                # Create cell with employee box if scheduled
                table_html += "<td>"
                if scheduled:
                    table_html += f"<div class='employee-box'>{employee_name}</div>"
                table_html += "</td>"
            
            table_html += "</tr>"
        
        table_html += "</table>"
        
        # Display the table
        st.markdown(table_html, unsafe_allow_html=True)

    # --- 3. Time Tracking Section ---
    with st.container():
        st.subheader("🕒 Time Tracking")
        
        # Get time entries for current week
        time_entries = session.sql(f"""
            SELECT 
                ENTRY_DATE,
                CLOCK_IN,
                CLOCK_OUT,
                TIMEDIFF('MINUTE', CLOCK_IN, CLOCK_OUT)/60.0 as hours_worked
            FROM employee_time_entries
            WHERE EMPLOYEEID = '{st.session_state.user_id}'
            AND ENTRY_DATE BETWEEN '{start_of_week}' AND '{end_of_week}'
            ORDER BY ENTRY_DATE DESC, CLOCK_IN DESC
        """).collect()
        
        if time_entries:
            # Prepare table data
            table_data = []
            total_hours = 0
            for entry in time_entries:
                hours = entry['HOURS_WORKED'] or 0
                total_hours += hours
                table_data.append({
                    "Date": entry['ENTRY_DATE'].strftime('%Y-%m-%d'),
                    "Clock In": entry['CLOCK_IN'].strftime('%I:%M %p') if entry['CLOCK_IN'] else "N/A",
                    "Clock Out": entry['CLOCK_OUT'].strftime('%I:%M %p') if entry['CLOCK_OUT'] else "N/A",
                    "Hours Worked": f"{hours:.2f}"
                })
            
            # Display table
            st.dataframe(
                pd.DataFrame(table_data),
                column_config={
                    "Date": st.column_config.Column(width="small"),
                    "Clock In": st.column_config.Column(width="small"),
                    "Clock Out": st.column_config.Column(width="small"),
                    "Hours Worked": st.column_config.Column(width="small")
                },
                hide_index=True,
                use_container_width=True
            )
            
            # Display totals
            st.metric("Total Hours Worked This Week", f"{total_hours:.2f}")
        else:
            st.info("No time records found for this week")

    # --- 4. Upcoming Appointments ---
    with st.container():
        st.subheader("📅 Upcoming Appointments")
        
        # Get appointments for next 7 days
        appointments = session.sql(f"""
            SELECT 
                c.name as customer_name,
                c.phone as customer_phone,
                c.address as customer_address,
                a.scheduled_time,
                TO_VARCHAR(a.scheduled_time, 'HH12:MI AM') AS time_formatted,
                a.notes
            FROM appointments a
            JOIN customers c ON a.customerid = c.customerid
            WHERE a.technicianid = '{st.session_state.user_id}'
            AND DATE(a.scheduled_time) BETWEEN CURRENT_DATE() AND DATEADD('day', 7, CURRENT_DATE())
            ORDER BY a.scheduled_time
        """).collect()
        
        if appointments:
            for appt in appointments:
                with st.expander(f"{appt['SCHEDULED_TIME'].strftime('%A %m/%d')} - {appt['CUSTOMER_NAME']} ({appt['TIME_FORMATTED']})"):
                    st.write(f"**Customer:** {appt['CUSTOMER_NAME']}")
                    st.write(f"**Phone:** {appt['CUSTOMER_PHONE']}")
                    st.write(f"**Address:** {appt['CUSTOMER_ADDRESS']}")
                    if appt['NOTES']:
                        st.write(f"**Notes:** {appt['NOTES']}")
        else:
            st.info("No appointments scheduled for the next 7 days")
      
          
 

######################################################################            
#######################################################################            
#######################################################################


#######################################################################
#######################################################################
#######################################################################
#######################################################################
# Customer management 

def customer_management():
    st.subheader("👥 Customer Management")
    session = get_session()

    # Initialize session state for form persistence
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
            'latitude': 0.0,
            'longitude': 0.0,
            'has_lock_box': 'No',
            'lock_box_code': '',
            'how_heard': '',
            'friend_name': '',
            'note': ''
        }

    # --- Add New Customer Section ---
    with st.expander("➕ Add New Customer", expanded=False):
        with st.form("add_customer_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            
            with col1:
                name = st.text_input("Full Name*", value=st.session_state.customer_form_data['name'])
                phone = st.text_input("Phone* (###-###-####)", value=st.session_state.customer_form_data['phone'], placeholder="301-555-1234")
                email = st.text_input("Email", value=st.session_state.customer_form_data['email'])
                address = st.text_input("Address*", value=st.session_state.customer_form_data['address'])
                unit = st.text_input("Unit/Apt", value=st.session_state.customer_form_data['unit'])
                
            with col2:
                city = st.text_input("City*", value=st.session_state.customer_form_data['city'])
                state = st.selectbox("State*", ["MD", "DC", "VA"], index=["MD", "DC", "VA"].index(st.session_state.customer_form_data['state']))
                zipcode = st.text_input("Zip Code* (5 or 9 digits)", value=st.session_state.customer_form_data['zipcode'])
                
                # How Heard section
                how_heard = st.selectbox(
                    "How did you hear about us?",
                    ["", "Google", "Friend", "Facebook", "Yelp", "Other"],
                    index=0
                )
                
                # Show friend name field only if "Friend" is selected
                friend_name = ""
                if how_heard == "Friend":
                    friend_name = st.text_input("Friend's Name*", value=st.session_state.customer_form_data['friend_name'])
                
                # Lock Box section
                has_lock_box = st.radio("Lock Box", ["No", "Yes"], index=0 if st.session_state.customer_form_data['has_lock_box'] == 'No' else 1, horizontal=True)
                
                # Show lock box code only if "Yes" is selected
                lock_box_code = ""
                if has_lock_box == "Yes":
                    lock_box_code = st.text_input("Lock Box Code*", value=st.session_state.customer_form_data['lock_box_code'])
            
            # Additional fields
            note = st.text_area("Note", value=st.session_state.customer_form_data['note'])

            # Submit button for the form
            submitted = st.form_submit_button("Add Customer")
            
            if submitted:
                # Store all values in session state for persistence
                st.session_state.customer_form_data = {
                    'name': name,
                    'phone': phone,
                    'email': email,
                    'address': address,
                    'unit': unit,
                    'city': city,
                    'state': state,
                    'zipcode': zipcode,
                    'latitude': 0.0,
                    'longitude': 0.0,
                    'has_lock_box': has_lock_box,
                    'lock_box_code': lock_box_code,
                    'how_heard': how_heard,
                    'friend_name': friend_name if how_heard == "Friend" else "",
                    'note': note
                }

                # Validate required fields
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
                if has_lock_box == "Yes" and not lock_box_code:
                    errors.append("Lock Box Code is required when Lock Box is Yes")
                if how_heard == "Friend" and not friend_name:
                    errors.append("Friend's Name is required when referral is from a friend")

                if errors:
                    for error in errors:
                        st.error(error)
                else:
                    try:
                        # Get the next customer ID
                        last_customer = session.sql("""
                            SELECT CUSTOMERID FROM customers 
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
                        
                        # Prepare how_heard value
                        how_heard_value = how_heard
                        if how_heard == "Friend":
                            how_heard_value = f"Friend: {friend_name}"
                        
                        # Build the insert query with proper string escaping
                        email_escaped = email.replace("'", "''") if email else None
                        unit_escaped = unit.replace("'", "''") if unit else None
                        lock_box_code_escaped = lock_box_code.replace("'", "''") if lock_box_code else None
                        how_heard_value_escaped = how_heard_value.replace("'", "''") if how_heard_value else None
                        note_escaped = note.replace("'", "''") if note else None
                        
                        insert_query = f"""
                            INSERT INTO customers 
                            (CUSTOMERID, NAME, PHONE, EMAIL, ADDRESS, UNIT, CITY, STATE, ZIPCODE,
                             LATITUDE, LONGITUDE, HAS_LOCK_BOX, LOCK_BOX_CODE, HOW_HEARD, NOTE)
                            VALUES (
                                '{customer_id}',
                                '{name.replace("'", "''")}',
                                '{phone.replace("'", "''")}',
                                {f"'{email_escaped}'" if email else 'NULL'},
                                '{address.replace("'", "''")}',
                                {f"'{unit_escaped}'" if unit else 'NULL'},
                                '{city.replace("'", "''")}',
                                '{state}',
                                '{zipcode}',
                                0.0,
                                0.0,
                                '{has_lock_box}',
                                {f"'{lock_box_code_escaped}'" if lock_box_code else 'NULL'},
                                {f"'{how_heard_value_escaped}'" if how_heard_value else 'NULL'},
                                {f"'{note_escaped}'" if note else 'NULL'}
                            )
                        """
                        
                        session.sql(insert_query).collect()
                        st.success(f"✅ Customer added successfully! Customer ID: {customer_id}")
                        
                        # Clear form data after successful submission
                        st.session_state.customer_form_data = {
                            'name': '',
                            'phone': '',
                            'email': '',
                            'address': '',
                            'unit': '',
                            'city': '',
                            'state': 'MD',
                            'zipcode': '',
                            'latitude': 0.0,
                            'longitude': 0.0,
                            'has_lock_box': 'No',
                            'lock_box_code': '',
                            'how_heard': '',
                            'friend_name': '',
                            'note': ''
                        }
                        
                        # Force a rerun to clear the form
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Error adding customer: {str(e)}")

    # --- Customer Search and Edit Section ---
    st.subheader("🔍 Search Customers")
    search_term = st.text_input("", placeholder="Search by name, phone, email, or address", key="unified_search")
    
    if search_term:
        customers = session.sql(f"""
            SELECT * FROM customers 
            WHERE NAME ILIKE '%{search_term}%' 
               OR PHONE ILIKE '%{search_term}%'
               OR EMAIL ILIKE '%{search_term}%'
               OR ADDRESS ILIKE '%{search_term}%'
            ORDER BY NAME
        """).collect()
        
        if customers:
            for customer in customers:
                with st.expander(f"{customer['NAME']} - {customer['PHONE']}"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write(f"**Customer ID:** {customer['CUSTOMERID']}")
                        st.write(f"**Email:** {customer['EMAIL'] or 'Not provided'}")
                        st.write(f"**Address:** {customer['ADDRESS']}")
                        
                    with col2:
                        st.write(f"**Lock Box:** {'Yes' if customer.get('HAS_LOCK_BOX', False) else 'No'}")
                        if customer.get('HAS_LOCK_BOX', False):
                            st.write(f"**Lock Box Code:** {customer.get('LOCK_BOX_CODE', 'Not provided')}")
                        st.write(f"**How Heard:** {customer.get('HOW_HEARD', 'Not specified')}")
                    
                    st.write(f"**Note:** {customer.get('NOTE', 'None')}")
                    
                    # Action buttons
                    if st.button("Edit", key=f"edit_{customer['CUSTOMERID']}"):
                        st.session_state['edit_customer'] = customer['CUSTOMERID']
                        st.session_state['customer_to_edit'] = customer

    # --- Edit Customer Form ---
    if 'edit_customer' in st.session_state and 'customer_to_edit' in st.session_state:
        edit_customer_id = st.session_state['edit_customer']
        customer_to_edit = st.session_state['customer_to_edit']
        
        st.subheader("✏️ Edit Customer")
        with st.form("edit_customer_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                name = st.text_input("Full Name*", value=customer_to_edit['NAME'])
                phone = st.text_input("Phone*", value=customer_to_edit['PHONE'])
                email = st.text_input("Email", value=customer_to_edit['EMAIL'] or "")
                address = st.text_input("Address*", value=customer_to_edit['ADDRESS'])
                
            with col2:
                # Parse how_heard value safely
                how_heard_value = customer_to_edit.get('HOW_HEARD', '') or ''
                if isinstance(how_heard_value, str):
                    if ":" in how_heard_value:
                        how_heard = how_heard_value.split(":")[0].strip()
                        friend_name = how_heard_value.split(":")[1].strip() if how_heard == "Friend" else ""
                    else:
                        how_heard = how_heard_value
                        friend_name = ""
                else:
                    how_heard = ""
                    friend_name = ""
                
                # How Heard section
                how_heard = st.selectbox(
                    "How did you hear about us?",
                    ["", "Google", "Friend", "Facebook", "Yelp", "Other"],
                    index=["", "Google", "Friend", "Facebook", "Yelp", "Other"].index(how_heard) if how_heard in ["", "Google", "Friend", "Facebook", "Yelp", "Other"] else 0
                )
                
                # Show friend name field only if "Friend" is selected
                edit_friend_name = ""
                if how_heard == "Friend":
                    edit_friend_name = st.text_input("Friend's Name*", value=friend_name)
                
                # Lock Box section
                has_lock_box = st.radio(
                    "Lock Box", 
                    ["No", "Yes"], 
                    horizontal=True,
                    index=1 if customer_to_edit.get('HAS_LOCK_BOX', False) else 0
                )
                
                # Show code input if Lock Box is Yes
                lock_box_code = ""
                if has_lock_box == "Yes":
                    lock_box_code = st.text_input("Lock Box Code*", value=customer_to_edit.get('LOCK_BOX_CODE', ''))
                
                note = st.text_area("Note", value=customer_to_edit.get('NOTE', ''))
            
            # Description/notes
            description = st.text_area("Customer Request", value=customer_to_edit.get('REQUEST', ''))
            
            # Form submit button
            submitted = st.form_submit_button("💾 Save Changes")
            
            if submitted:
                # Validate inputs
                if not all([name, phone, address]):
                    st.error("Please fill in all required fields (*)")
                elif not re.match(r"^\d{3}-\d{3}-\d{4}$", phone):
                    st.error("Invalid phone number format. Please use ###-###-####")
                elif has_lock_box == "Yes" and not lock_box_code:
                    st.error("Please enter Lock Box Code when Lock Box is set to Yes")
                elif how_heard == "Friend" and not edit_friend_name:
                    st.error("Please enter Friend's Name when referral is from a friend")
                else:
                    try:
                        # Prepare how_heard value
                        how_heard_value = how_heard
                        if how_heard == "Friend":
                            how_heard_value = f"Friend: {edit_friend_name}"
                        
                        # Prepare SQL values with proper escaping
                        email_escaped = email.replace("'", "''") if email else None
                        lock_box_code_escaped = lock_box_code.replace("'", "''") if lock_box_code else None
                        how_heard_value_escaped = how_heard_value.replace("'", "''") if how_heard_value else None
                        note_escaped = note.replace("'", "''") if note else None
                        description_escaped = description.replace("'", "''") if description else None
                        
                        # Update customer record
                        update_query = f"""
                            UPDATE customers 
                            SET NAME = '{name.replace("'", "''")}',
                                PHONE = '{phone.replace("'", "''")}',
                                EMAIL = {f"'{email_escaped}'" if email else 'NULL'},
                                ADDRESS = '{address.replace("'", "''")}',
                                HAS_LOCK_BOX = {'TRUE' if has_lock_box == 'Yes' else 'FALSE'},
                                LOCK_BOX_CODE = {f"'{lock_box_code_escaped}'" if lock_box_code else 'NULL'},
                                HOW_HEARD = {f"'{how_heard_value_escaped}'" if how_heard_value else 'NULL'},
                                NOTE = {f"'{note_escaped}'" if note else 'NULL'},
                                REQUEST = {f"'{description_escaped}'" if description else 'NULL'}
                            WHERE CUSTOMERID = '{edit_customer_id}'
                        """
                        
                        session.sql(update_query).collect()
                        st.success("Customer updated successfully!")
                        del st.session_state['edit_customer']
                        del st.session_state['customer_to_edit']
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error updating customer: {str(e)}")
            
            # Cancel button
            if st.button("❌ Cancel"):
                del st.session_state['edit_customer']
                del st.session_state['customer_to_edit']
                st.rerun()
#######################################################################
#######################################################################
#######################################################################
#######################################################################
######################################################################

#Appointment 

def appointments():
    st.subheader("📅 Appointment Scheduling")
    session = get_session()

    # --- Step 1: Customer Selection ---
    st.subheader("1. Select Customer")
    search_query = st.text_input("Search by Name, Phone, Email, or Address", key="customer_search")
    
    # Fetch customers
    customers = session.sql(f"""
        SELECT customerid, name, phone FROM customers 
        {'WHERE NAME ILIKE ' + f"'%{search_query}%'" if search_query else ''}
        ORDER BY name
    """).collect()
    
    if not customers:
        st.warning("No customers found")
        return
    
    selected_customer_id = st.selectbox(
        "Select Customer",
        options=[row['CUSTOMERID'] for row in customers],
        format_func=lambda x: next(f"{row['NAME']} ({row['PHONE']})" for row in customers if row['CUSTOMERID'] == x)
    )

    # --- Step 2: Service Request ---
    st.subheader("2. Service Request")
    request_type = st.selectbox(
        "Select Request Type",
        ["Install", "Service", "Estimate"],
        index=0
    )

    # Get qualified technicians
    expertise_map = {"Install": "EX1", "Service": "EX2", "Estimate": "EX3"}
    technicians = session.sql(f"""
        SELECT e.employeeid, e.ename 
        FROM employees e
        JOIN employee_expertise ee ON e.employeeid = ee.employeeid
        WHERE ee.expertiseid = '{expertise_map[request_type]}'
    """).collect()
    
    if not technicians:
        st.error("No technicians available")
        return

    # --- Different Logic for Install vs Other Services ---
    if request_type == "Install":
        # Full-day booking for installations
        st.subheader("3. Select Installation Date")
        
        # Show 4 weeks of available dates
        start_date = datetime.now().date()
        dates = [start_date + timedelta(days=i) for i in range(28)]
        
        # Get already booked installation days
        booked_days = session.sql(f"""
            SELECT DISTINCT DATE(scheduled_time) as day 
            FROM appointments 
            WHERE service_type = 'Install'
            AND DATE(scheduled_time) BETWEEN '{start_date}' AND '{start_date + timedelta(days=28)}'
        """).collect()
        booked_days = [row['DAY'] for row in booked_days]
        
        # Display available dates
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
        
        # Handle installation booking
        if 'selected_install_date' in st.session_state:
            date = st.session_state.selected_install_date
            st.success(f"Selected installation date: {date.strftime('%A, %B %d')}")
            
            # Select primary technician
            primary_tech = st.selectbox(
                "Primary Technician",
                options=[t['EMPLOYEEID'] for t in technicians],
                format_func=lambda x: next(t['ENAME'] for t in technicians if t['EMPLOYEEID'] == x)
            )
            
            # Select secondary technician (optional)
            secondary_techs = [t for t in technicians if t['EMPLOYEEID'] != primary_tech]
            secondary_tech = st.selectbox(
                "Additional Technician (Optional)",
                options=[""] + [t['EMPLOYEEID'] for t in secondary_techs],
                format_func=lambda x: next(t['ENAME'] for t in technicians if t['EMPLOYEEID'] == x) if x else "None"
            )
            
            notes = st.text_area("Installation Notes")
            
            if st.button("Book Installation"):
                try:
                    # Book primary technician for full day (8AM-5PM)
                    session.sql(f"""
                        INSERT INTO appointments (
                            appointmentid, customerid, technicianid,
                            scheduled_time, service_type, notes, sta_tus
                        ) VALUES (
                            'APT{datetime.now().timestamp()}',
                            '{selected_customer_id}',
                            '{primary_tech}',
                            '{datetime.combine(date, time(8,0))}',
                            'Install',
                            '{notes}',
                            'scheduled'
                        )
                    """).collect()
                    
                    # Book secondary technician if selected
                    if secondary_tech:
                        session.sql(f"""
                            INSERT INTO appointments (
                                appointmentid, customerid, technicianid,
                                scheduled_time, service_type, notes, sta_tus
                            ) VALUES (
                                'APT{datetime.now().timestamp()}',
                                '{selected_customer_id}',
                                '{secondary_tech}',
                                '{datetime.combine(date, time(8,0))}',
                                'Install-Assist',
                                '{notes}',
                                'scheduled'
                            )
                        """).collect()
                    
                    st.success(f"Installation booked for {date.strftime('%A, %B %d')}!")
                    del st.session_state.selected_install_date
                    st.rerun()
                
                except Exception as e:
                    st.error(f"Error: {str(e)}")

    else:
        # Standard 2-hour slots for non-installation services
        st.subheader("3. Select Appointment Time")
        
        # Week navigation
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
        
        # Get existing appointments
        appointments = session.sql(f"""
            SELECT * FROM appointments
            WHERE DATE(scheduled_time) BETWEEN '{start_date}' AND '{start_date + timedelta(days=6)}'
            AND sta_tus != 'cancelled'
        """).collect()
        
        # Create calendar with 2-hour slots (8AM-6PM)
        time_slots = [time(hour) for hour in range(8, 19, 2)]  # 8AM, 10AM, 12PM, 2PM, 4PM, 6PM
        
        for day in days:
            with st.expander(day.strftime("%A %m/%d"), expanded=True):
                cols = st.columns(len(time_slots))
                
                for i, time_slot in enumerate(time_slots):
                    slot_start = datetime.combine(day, time_slot)
                    slot_end = slot_start + timedelta(hours=2)
                    
                    with cols[i]:
                        # Check technician availability
                        available_techs = []
                        for tech in technicians:
                            tech_id = tech['EMPLOYEEID']
                            
                            # Check for overlapping appointments
                            is_busy = any(
                                a for a in appointments 
                                if a['TECHNICIANID'] == tech_id
                                and datetime.combine(day, a['SCHEDULED_TIME'].time()) < slot_end
                                and (datetime.combine(day, a['SCHEDULED_TIME'].time()) + timedelta(hours=2)) > slot_start
                            )
                            
                            if not is_busy:
                                available_techs.append(tech)
                        
                        # Display time slot (8-10 format)
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
                        else:
                            st.button(
                                slot_label,
                                disabled=True,
                                key=f"slot_{day}_{time_slot}_disabled"
                            )
        
        # Handle slot selection for non-install services
        if 'selected_slot' in st.session_state:
            slot = st.session_state.selected_slot
            time_range = f"{slot['datetime'].hour}-{slot['datetime'].hour+2}"
            st.success(f"Selected: {slot['datetime'].strftime('%A %m/%d')} {time_range}")
            
            # Primary technician selection
            primary_tech = st.selectbox(
                "Primary Technician",
                options=[t['EMPLOYEEID'] for t in slot['techs']],
                format_func=lambda x: next(t['ENAME'] for t in slot['techs'] if t['EMPLOYEEID'] == x)
            )
            
            # Secondary technician selection (optional)
            secondary_techs = [t for t in slot['techs'] if t['EMPLOYEEID'] != primary_tech]
            secondary_tech = st.selectbox(
                "Additional Technician (Optional)",
                options=[""] + [t['EMPLOYEEID'] for t in secondary_techs],
                format_func=lambda x: next(t['ENAME'] for t in slot['techs'] if t['EMPLOYEEID'] == x) if x else "None"
            )
            
            notes = st.text_area("Service Notes")
            
            if st.button("Book Appointment"):
                try:
                    # Check availability again
                    existing = session.sql(f"""
                        SELECT * FROM appointments
                        WHERE technicianid = '{primary_tech}'
                        AND DATE(scheduled_time) = '{slot['datetime'].date()}'
                        AND HOUR(scheduled_time) = {slot['datetime'].hour}
                        AND sta_tus != 'cancelled'
                    """).collect()
                    
                    if existing:
                        st.error("Time slot no longer available")
                        del st.session_state.selected_slot
                        st.rerun()
                    
                    # Book primary technician
                    session.sql(f"""
                        INSERT INTO appointments (
                            appointmentid, customerid, technicianid, 
                            scheduled_time, service_type, notes, sta_tus
                        ) VALUES (
                            'APT{datetime.now().timestamp()}',
                            '{selected_customer_id}',
                            '{primary_tech}',
                            '{slot['datetime']}',
                            '{request_type}',
                            '{notes}',
                            'scheduled'
                        )
                    """).collect()
                    
                    # Book secondary technician if selected
                    if secondary_tech:
                        session.sql(f"""
                            INSERT INTO appointments (
                                appointmentid, customerid, technicianid, 
                                scheduled_time, service_type, notes, sta_tus
                            ) VALUES (
                                'APT{datetime.now().timestamp()}',
                                '{selected_customer_id}',
                                '{secondary_tech}',
                                '{slot['datetime']}',
                                '{request_type}-Assist',
                                '{notes}',
                                'scheduled'
                            )
                        """).collect()
                    
                    st.success(f"Appointment booked for {time_range}!")
                    del st.session_state.selected_slot
                    st.rerun()
                
                except Exception as e:
                    st.error(f"Error: {str(e)}")

    # --- Current Appointments Display ---
    st.subheader("Current Appointments This Week")
    current_appts = session.sql(f"""
        SELECT 
            a.appointmentid,
            c.name as customer_name,
            e.ename as technician_name,
            a.scheduled_time,
            a.service_type,
            a.sta_tus,
            a.notes
        FROM appointments a
        JOIN customers c ON a.customerid = c.customerid
        JOIN employees e ON a.technicianid = e.employeeid
        WHERE DATE(a.scheduled_time) BETWEEN '{start_date}' AND '{start_date + timedelta(days=6)}'
        ORDER BY a.scheduled_time
    """).collect()
    
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


    
#######################################################################
#######################################################################
def equipment_management():
    st.subheader("🛠️ ")
    session = get_session()

    # Search and select a customer
    st.subheader("Select Customer")
    search_query = st.text_input("Search by Name or Phone")

    # Fetch customers based on search query
    if search_query:
        customers = session.sql(f"""
            SELECT c.*, cu.* 
            FROM customers c
            LEFT JOIN customer_units cu ON c.customerid = cu.customerid
            WHERE c.NAME ILIKE '%{search_query}%' OR c.PHONE ILIKE '%{search_query}%'
        """).collect()
    else:
        customers = session.sql("""
            SELECT c.*, cu.* 
            FROM customers c
            LEFT JOIN customer_units cu ON c.customerid = cu.customerid
        """).collect()

    if not customers:
        st.warning("No customers found.")
        return

    customer_options = {row['CUSTOMERID']: f"{row['NAME']} ({row['PHONE']})" for row in customers}
    selected_customer_id = st.selectbox(
        "Select Customer",
        options=customer_options.keys(),
        format_func=lambda x: customer_options[x]
    )

    if not selected_customer_id:
        return

    # Get selected customer details
    selected_customer = next((c for c in customers if c['CUSTOMERID'] == selected_customer_id), None)
    
    if not selected_customer:
        st.error("Customer not found")
        return

    # Display customer info header
    st.write(f"### Equipment for {selected_customer['NAME']}")
    st.write(f"**Address:** {selected_customer['ADDRESS']}")
    st.write(f"**Phone:** {selected_customer['PHONE']}")

    # Check if equipment info exists
    if not selected_customer['UNITID']:
        st.warning("No equipment information recorded for this customer.")
        
        # Add new equipment form
        with st.expander("➕ Add New Equipment Record"):
            with st.form("add_equipment_form"):
                st.subheader("Outdoor Unit")
                outdoor_unit = st.text_input("Outdoor Unit Model")
                outdoor_unit_age = st.number_input("Outdoor Unit Age (years)", min_value=0, max_value=50, step=1)
                
                st.subheader("Indoor Unit")
                indoor_unit = st.text_input("Indoor Unit Model")
                indoor_unit_age = st.number_input("Indoor Unit Age (years)", min_value=0, max_value=50, step=1)
                
                st.subheader("Thermostat")
                thermostat_type = st.text_input("Thermostat Type")
                thermostat_age = st.number_input("Thermostat Age (years)", min_value=0, max_value=50, step=1)
                
                st.subheader("Location & Accessibility")
                unit_location = st.selectbox(
                    "Unit Location",
                    ["Attic", "Basement", "Garage", "Closet", "Crawlspace", "Other"]
                )
                accessibility_level = st.selectbox(
                    "Accessibility Level",
                    ["Easy", "Moderate", "Difficult", "Very Difficult"]
                )
                accessibility_notes = st.text_area("Accessibility Notes")
                other_notes = st.text_area("Other Notes")
                
                if st.form_submit_button("Save Equipment Record"):
                    unit_id = f"UNIT{datetime.now().timestamp()}"
                    session.sql(f"""
                        INSERT INTO customer_units 
                        (UNITID, CUSTOMERID, OUTDOOR_UNIT, OUTDOOR_UNIT_AGE, 
                         INDOOR_UNIT, INDOOR_UNIT_AGE, THERMOSTAT_TYPE, THERMOSTAT_AGE,
                         UNIT_LOCATION, ACCESSIBILITY_LEVEL, ACCESSIBILITY_NOTES, OTHER_NOTES)
                        VALUES (
                            '{unit_id}',
                            '{selected_customer_id}',
                            '{outdoor_unit}',
                            {outdoor_unit_age},
                            '{indoor_unit}',
                            {indoor_unit_age},
                            '{thermostat_type}',
                            {thermostat_age},
                            '{unit_location}',
                            '{accessibility_level}',
                            '{accessibility_notes}',
                            '{other_notes}'
                        )
                    """).collect()
                    st.success("Equipment record added successfully!")
                    st.rerun()
    else:
        # Display existing equipment info
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Outdoor Unit")
            st.write(f"**Model:** {selected_customer['OUTDOOR_UNIT'] or 'Not recorded'}")
            st.write(f"**Age:** {selected_customer['OUTDOOR_UNIT_AGE'] or 'N/A'} years")
            
            st.subheader("Indoor Unit")
            st.write(f"**Model:** {selected_customer['INDOOR_UNIT'] or 'Not recorded'}")
            st.write(f"**Age:** {selected_customer['INDOOR_UNIT_AGE'] or 'N/A'} years")
            
        with col2:
            st.subheader("Thermostat")
            st.write(f"**Type:** {selected_customer['THERMOSTAT_TYPE'] or 'Not recorded'}")
            st.write(f"**Age:** {selected_customer['THERMOSTAT_AGE'] or 'N/A'} years")
            
            st.subheader("Location")
            st.write(f"**Location:** {selected_customer['UNIT_LOCATION'] or 'Not recorded'}")
            st.write(f"**Accessibility:** {selected_customer['ACCESSIBILITY_LEVEL'] or 'Not recorded'}")
        
        st.subheader("Additional Notes")
        st.write(f"**Accessibility Notes:**")
        st.info(selected_customer['ACCESSIBILITY_NOTES'] or "No notes available")
        st.write(f"**Other Notes:**")
        st.info(selected_customer['OTHER_NOTES'] or "No notes available")

        # Edit equipment info
        with st.expander("✏️ Edit Equipment Information"):
            with st.form("edit_equipment_form"):
                st.subheader("Outdoor Unit")
                outdoor_unit = st.text_input("Outdoor Unit Model", value=selected_customer['OUTDOOR_UNIT'] or "")
                outdoor_unit_age = st.number_input("Outdoor Unit Age (years)", 
                                                 min_value=0, max_value=50, step=1,
                                                 value=selected_customer['OUTDOOR_UNIT_AGE'] or 0)
                
                st.subheader("Indoor Unit")
                indoor_unit = st.text_input("Indoor Unit Model", value=selected_customer['INDOOR_UNIT'] or "")
                indoor_unit_age = st.number_input("Indoor Unit Age (years)", 
                                                min_value=0, max_value=50, step=1,
                                                value=selected_customer['INDOOR_UNIT_AGE'] or 0)
                
                st.subheader("Thermostat")
                thermostat_type = st.text_input("Thermostat Type", value=selected_customer['THERMOSTAT_TYPE'] or "")
                thermostat_age = st.number_input("Thermostat Age (years)", 
                                              min_value=0, max_value=50, step=1,
                                              value=selected_customer['THERMOSTAT_AGE'] or 0)
                
                st.subheader("Location & Accessibility")
                unit_location = st.selectbox(
                    "Unit Location",
                    ["Attic", "Basement", "Garage", "Closet", "Crawlspace", "Other"],
                    index=["Attic", "Basement", "Garage", "Closet", "Crawlspace", "Other"].index(
                        selected_customer['UNIT_LOCATION'] or "Attic")
                )
                accessibility_level = st.selectbox(
                    "Accessibility Level",
                    ["Easy", "Moderate", "Difficult", "Very Difficult"],
                    index=["Easy", "Moderate", "Difficult", "Very Difficult"].index(
                        selected_customer['ACCESSIBILITY_LEVEL'] or "Easy")
                )
                accessibility_notes = st.text_area("Accessibility Notes", 
                                                 value=selected_customer['ACCESSIBILITY_NOTES'] or "")
                other_notes = st.text_area("Other Notes", 
                                         value=selected_customer['OTHER_NOTES'] or "")
                
                if st.form_submit_button("Update Equipment Record"):
                    session.sql(f"""
                        UPDATE customer_units 
                        SET OUTDOOR_UNIT = '{outdoor_unit}',
                            OUTDOOR_UNIT_AGE = {outdoor_unit_age},
                            INDOOR_UNIT = '{indoor_unit}',
                            INDOOR_UNIT_AGE = {indoor_unit_age},
                            THERMOSTAT_TYPE = '{thermostat_type}',
                            THERMOSTAT_AGE = {thermostat_age},
                            UNIT_LOCATION = '{unit_location}',
                            ACCESSIBILITY_LEVEL = '{accessibility_level}',
                            ACCESSIBILITY_NOTES = '{accessibility_notes}',
                            OTHER_NOTES = '{other_notes}'
                        WHERE CUSTOMERID = '{selected_customer_id}'
                    """).collect()
                    st.success("Equipment record updated successfully!")
                    st.rerun()



#######################################################################
#######################################################################
# Quotes/ Invoices

def quotes():
    session = get_session()
    st.title("Quotes & Invoices")
    
    # Initialize session state for quote items if not exists
    if 'quote_items' not in st.session_state:
        st.session_state.quote_items = []
    
    # Create two tabs
    tab_repair, tab_install = st.tabs(["Repair Services", "New Installation"])

    with tab_repair:
        st.header("Repair Service Quote")
        
        # Customer selection
        customers = session.sql("SELECT customerid, name, phone, address, email FROM customers").collect()
        customer_options = {row['CUSTOMERID']: f"{row['NAME']} ({row['PHONE']})" for row in customers}
        
        selected_customer_id = st.selectbox(
            "Select Customer",
            options=customer_options.keys(),
            format_func=lambda x: customer_options[x],
            key="repair_customer"
        )
        
        # Get selected customer details
        customer_info = next((c for c in customers if c['CUSTOMERID'] == selected_customer_id), None)
        
        # Date field
        quote_date = st.date_input("Quote Date", value=datetime.now().date(), key="repair_date")
        
        # Repair services with prices (can select multiple)
        repair_services = {
            "Condenser service": 85,
            "Replacing capacitor": 150,
            "Adding refrigerant": 89,
            "Leak detection": 125,
            "Replacing fan motor": 200,
            "Compressor replacement": 500,
            "System diagnostic": 75,
            "Thermostat replacement": 120
        }
        
        selected_services = st.multiselect(
            "Select Repair Services",
            options=list(repair_services.keys()),
            key="repair_services"
        )
        
        # Service price editor
        service_prices = {}
        cols = st.columns(2)
        for i, service in enumerate(selected_services):
            with cols[i % 2]:
                service_prices[service] = st.number_input(
                    f"Price for {service} ($)",
                    min_value=0.0,
                    value=float(repair_services[service]),
                    step=1.0,
                    key=f"service_{service}"
                )
        
        # Button to add services to quote
        if st.button("Add Services to Quote", key="add_services"):
            for service, price in service_prices.items():
                st.session_state.quote_items.append({
                    'type': 'Service',
                    'description': service,
                    'price': price,
                    'quantity': 1
                })
            st.success("Services added to quote!")
        
        # Materials section
        st.subheader("Materials")
        materials = {
            "Refrigerant (per lb)": 25,
            "Filter": 15,
            "Wire (per ft)": 2,
            "Water line (per ft)": 3,
            "Capacitor": 45,
            "Contactor": 60,
            "Fan motor": 120,
            "Thermostat": 85,
            "Circuit board": 195,
            "Ductwork (per ft)": 12
        }
        
        selected_materials = st.multiselect(
            "Select Materials Used", 
            options=list(materials.keys()),
            key="repair_materials"
        )
        
        # Material price and quantity editor
        material_prices = {}
        material_quantities = {}
        cols = st.columns(3)
        for i, material in enumerate(selected_materials):
            with cols[i % 3]:
                material_prices[material] = st.number_input(
                    f"Unit Price for {material} ($)",
                    min_value=0.0,
                    value=float(materials[material]),
                    step=1.0,
                    key=f"material_price_{material}"
                )
                material_quantities[material] = st.number_input(
                    f"Qty for {material}",
                    min_value=1,
                    value=1,
                    key=f"material_qty_{material}"
                )
        
        # Button to add materials to quote
        if st.button("Add Materials to Quote", key="add_materials"):
            for material in selected_materials:
                st.session_state.quote_items.append({
                    'type': 'Material',
                    'description': material,
                    'price': material_prices[material],
                    'quantity': material_quantities[material]
                })
            st.success("Materials added to quote!")
        
        # Labor and fees section
        st.subheader("Labor & Fees")
        labor_cost = st.number_input("Labor Cost ($)", min_value=0.0, value=85.0, step=1.0, key="repair_labor")
        tax_rate = st.number_input("Tax Rate (%)", min_value=0.0, max_value=100.0, value=6.0, step=0.1, key="repair_tax")
        
        # Button to add labor to quote
        if st.button("Add Labor to Quote", key="add_labor"):
            st.session_state.quote_items.append({
                'type': 'Labor',
                'description': 'Labor',
                'price': labor_cost,
                'quantity': 1
            })
            st.success("Labor added to quote!")
        
        # Description/notes
        description = st.text_area("Service Description", key="repair_description")
        
        # Display the quote table
        st.subheader("Current Quote Items")
        if st.session_state.quote_items:
            # Convert to dataframe for nice display
            import pandas as pd
            quote_df = pd.DataFrame(st.session_state.quote_items)
            
            # Calculate line totals
            quote_df['Line Total'] = quote_df['price'] * quote_df['quantity']
            
            # Display the table with totals
            st.dataframe(quote_df.style.format({
                'price': '${:.2f}',
                'Line Total': '${:.2f}'
            }), use_container_width=True)
            
            # Calculate subtotal, tax, and total
            subtotal = quote_df['Line Total'].sum()
            tax = subtotal * (tax_rate / 100)
            total = subtotal + tax
            
            # Display totals
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Subtotal", f"${subtotal:.2f}")
                st.metric("Tax", f"${tax:.2f}")
            with col2:
                st.metric("Total", f"${total:.2f}", delta_color="off")
            
            # Buttons to manage quote
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("Remove Last Item", key="remove_item"):
                    if st.session_state.quote_items:
                        st.session_state.quote_items.pop()
                        st.rerun()
            with col2:
                if st.button("Clear All Items", key="clear_items"):
                    st.session_state.quote_items = []
                    st.rerun()
            with col3:
                if st.button("Finalize Quote", key="finalize_repair"):
                    try:
                        # First ensure the QUANTITY column exists in quote_items table
                        try:
                            session.sql("ALTER TABLE quote_items ADD COLUMN IF NOT EXISTS QUANTITY NUMBER").collect()
                        except:
                            pass  # Column may already exist
                        
                        # Generate quote ID
                        quote_id = f"RQ_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                        
                        # First ensure the columns exist in customers table
                        try:
                            session.sql("ALTER TABLE customers ADD COLUMN IF NOT EXISTS LAST_QUOTE_ID VARCHAR(50)").collect()
                            session.sql("ALTER TABLE customers ADD COLUMN IF NOT EXISTS LAST_QUOTE_DATE TIMESTAMP").collect()
                        except:
                            pass  # Columns may already exist
                        
                        # Create quote record
                        session.sql(f"""
                            INSERT INTO quotes 
                            (QUOTEID, CUSTOMERID, TOTAL_AMOUNT, DESCRIPTION, QUOTE_NUMBER, STATUS, CREATED_AT)
                            VALUES (
                                '{quote_id}',
                                '{selected_customer_id}',
                                {total},
                                '{description.replace("'", "''")}',
                                'QUOTE-{datetime.now().strftime('%m%d%y')}-{len(st.session_state.quote_items)}',
                                'draft',
                                '{datetime.now()}'
                            )
                        """).collect()
                        
                        # Add all quote items
                        for item in st.session_state.quote_items:
                            session.sql(f"""
                                INSERT INTO quote_items (QUOTEID, ITEM_TYPE, ITEM_ID, PRICE, QUANTITY)
                                VALUES (
                                    '{quote_id}',
                                    '{item['type'].upper()}',
                                    '{item['description'].replace("'", "''")}',
                                    {item['price']},
                                    {item['quantity']}
                                )
                            """).collect()
                        
                        # Add tax as a separate item
                        session.sql(f"""
                            INSERT INTO quote_items (QUOTEID, ITEM_TYPE, ITEM_ID, PRICE, QUANTITY)
                            VALUES (
                                '{quote_id}',
                                'TAX',
                                'Sales Tax ({tax_rate}%)',
                                {tax},
                                1
                            )
                        """).collect()
                        
                        # Update customer record
                        session.sql(f"""
                            UPDATE customers 
                            SET LAST_QUOTE_ID = '{quote_id}',
                                LAST_QUOTE_DATE = '{datetime.now()}'
                            WHERE CUSTOMERID = '{selected_customer_id}'
                        """).collect()
                        
                        st.success("Quote created successfully!")
                        
                        # Generate the quote document
                        quote_doc = f"""
POTOMAC HVAC LLC
(301)825-4447

Invoice / Quote Number: QUOTE-{datetime.now().strftime('%m%d%y')}-{len(st.session_state.quote_items)}
Date: {quote_date.strftime('%B %d, %Y')}

Dear Customer Information:
Name: {customer_info['NAME']}
Phone: {customer_info['PHONE']}
Address: {customer_info['ADDRESS']}

Service Requested: {description or 'Various repair services'}

Itemized Quote:
{chr(10).join(f"- {item['type']}: {item['description']} ({item['quantity']} x ${item['price']:.2f}) = ${item['price'] * item['quantity']:.2f}" for item in st.session_state.quote_items)}
- Tax: ${tax:.2f}
----------------------------------
Total: ${total:.2f}

Payment:
To initiate the service, we require a deposit of half the total price. 
The service will commence within five business days of receiving the deposit. 
The remaining balance is due upon completion of the service.

We accept payment by check, cash, money order, Zelle, Venmo, and credit card (with a 3% fee).

Warranty:
We believe in the quality of our work and the products we use. 
All parts come with a 10-year warranty by the manufacturer after registration. 
Please note that labor costs are not included in this warranty. 
However, our workmanship warranty is for a lifetime, covering any problems related to the installation.

We truly appreciate your business and the trust you've placed in us.

Potomac HVAC LLC
(301)825-4447
"""
                        
                        # Store the quote document for download/email
                        st.session_state.current_quote = quote_doc
                        st.session_state.current_quote_id = quote_id
                        st.session_state.current_customer_email = customer_info['EMAIL']
                        st.session_state.quote_items = []  # Clear the quote items after finalizing
                        
                    except Exception as e:
                        st.error(f"Error creating quote: {str(e)}")
        else:
            st.info("No items added to quote yet. Add services or materials above.")

    with tab_install:
        st.header("New Installation Quote")
        
        # Customer selection
        selected_customer_id = st.selectbox(
            "Select Customer",
            options=customer_options.keys(),
            format_func=lambda x: customer_options[x],
            key="install_customer"
        )
        
        # Get selected customer details
        customer_info = next((c for c in customers if c['CUSTOMERID'] == selected_customer_id), None)
        
        # Date field
        quote_date = st.date_input("Quote Date", value=datetime.now().date(), key="install_date")
        
        # Installation dropdown
        brands = ["Carrier", "Bryant", "Trane", "Lennox", "Rheem", "Other"]
        selected_brand = st.selectbox("Select Unit Brand", options=brands, key="install_brand")
        
        col1, col2 = st.columns(2)
        with col1:
            unit_size = st.number_input("Unit Size (Ton)", min_value=1.0, max_value=5.0, step=0.5, key="install_size")
        with col2:
            unit_price = st.number_input("Unit Price ($)", min_value=0.0, value=4500.0, step=1.0, key="install_price")
        
        # Button to add unit to quote
        if st.button("Add Unit to Quote", key="add_unit"):
            st.session_state.quote_items.append({
                'type': 'Equipment',
                'description': f"{selected_brand} {unit_size} Ton Unit",
                'price': unit_price,
                'quantity': 1
            })
            st.success("Unit added to quote!")
        
        # Installation materials
        st.subheader("Installation Materials")
        install_materials = {
            "Line set (per ft)": 8,
            "Thermostat wire (per ft)": 1,
            "Ductwork (per ft)": 12,
            "Insulation (per ft)": 3,
            "Pad": 85,
            "Disconnect box": 65,
            "Condensate pump": 120,
            "Vent pipe (per ft)": 5
        }
        
        selected_install_materials = st.multiselect(
            "Select Installation Materials", 
            options=list(install_materials.keys()),
            key="install_materials"
        )
        
        # Material price and quantity editor
        install_material_prices = {}
        install_material_quantities = {}
        cols = st.columns(3)
        for i, material in enumerate(selected_install_materials):
            with cols[i % 3]:
                install_material_prices[material] = st.number_input(
                    f"Unit Price for {material} ($)",
                    min_value=0.0,
                    value=float(install_materials[material]),
                    step=1.0,
                    key=f"install_mat_price_{material}"
                )
                install_material_quantities[material] = st.number_input(
                    f"Qty for {material}",
                    min_value=1,
                    value=1,
                    key=f"install_mat_qty_{material}"
                )
        
        # Button to add materials to quote
        if st.button("Add Materials to Quote", key="add_install_materials"):
            for material in selected_install_materials:
                st.session_state.quote_items.append({
                    'type': 'Material',
                    'description': material,
                    'price': install_material_prices[material],
                    'quantity': install_material_quantities[material]
                })
            st.success("Materials added to quote!")
        
        # Installation labor and fees
        st.subheader("Installation Labor & Fees")
        install_labor_cost = st.number_input("Installation Labor Cost ($)", min_value=0.0, value=850.0, step=1.0, key="install_labor")
        install_tax_rate = st.number_input("Tax Rate (%)", min_value=0.0, max_value=100.0, value=6.0, step=0.1, key="install_tax")
        
        # Button to add labor to quote
        if st.button("Add Labor to Quote", key="add_install_labor"):
            st.session_state.quote_items.append({
                'type': 'Labor',
                'description': 'Installation Labor',
                'price': install_labor_cost,
                'quantity': 1
            })
            st.success("Labor added to quote!")
        
        # Installation description
        install_description = st.text_area("Installation Details", key="install_description")
        
        # Display the quote table (shared between both tabs)
        st.subheader("Current Quote Items")
        if st.session_state.quote_items:
            # Convert to dataframe for nice display
            import pandas as pd
            quote_df = pd.DataFrame(st.session_state.quote_items)
            
            # Calculate line totals
            quote_df['Line Total'] = quote_df['price'] * quote_df['quantity']
            
            # Display the table with totals
            st.dataframe(quote_df.style.format({
                'price': '${:.2f}',
                'Line Total': '${:.2f}'
            }), use_container_width=True)
            
            # Calculate subtotal, tax, and total
            subtotal = quote_df['Line Total'].sum()
            tax = subtotal * (install_tax_rate / 100)
            total = subtotal + tax
            
            # Display totals
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Subtotal", f"${subtotal:.2f}")
                st.metric("Tax", f"${tax:.2f}")
            with col2:
                st.metric("Total", f"${total:.2f}", delta_color="off")
            
            # Buttons to manage quote
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("Remove Last Item", key="remove_install_item"):
                    if st.session_state.quote_items:
                        st.session_state.quote_items.pop()
                        st.rerun()
            with col2:
                if st.button("Clear All Items", key="clear_install_items"):
                    st.session_state.quote_items = []
                    st.rerun()
            with col3:
                if st.button("Finalize Quote", key="finalize_install"):
                    try:
                        # First ensure the QUANTITY column exists in quote_items table
                        try:
                            session.sql("ALTER TABLE quote_items ADD COLUMN IF NOT EXISTS QUANTITY NUMBER").collect()
                        except:
                            pass  # Column may already exist
                        
                        # Generate quote ID
                        quote_id = f"IQ_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                        
                        # First ensure the columns exist in customers table
                        try:
                            session.sql("ALTER TABLE customers ADD COLUMN IF NOT EXISTS LAST_QUOTE_ID VARCHAR(50)").collect()
                            session.sql("ALTER TABLE customers ADD COLUMN IF NOT EXISTS LAST_QUOTE_DATE TIMESTAMP").collect()
                        except:
                            pass  # Columns may already exist
                        
                        # Create quote record
                        session.sql(f"""
                            INSERT INTO quotes 
                            (QUOTEID, CUSTOMERID, TOTAL_AMOUNT, DESCRIPTION, QUOTE_NUMBER, STATUS, CREATED_AT)
                            VALUES (
                                '{quote_id}',
                                '{selected_customer_id}',
                                {total},
                                '{install_description.replace("'", "''")}',
                                'INST-{datetime.now().strftime('%m%d%y')}-{unit_size}',
                                'draft',
                                '{datetime.now()}'
                            )
                        """).collect()
                        
                        # Add all quote items
                        for item in st.session_state.quote_items:
                            session.sql(f"""
                                INSERT INTO quote_items (QUOTEID, ITEM_TYPE, ITEM_ID, PRICE, QUANTITY)
                                VALUES (
                                    '{quote_id}',
                                    '{item['type'].upper()}',
                                    '{item['description'].replace("'", "''")}',
                                    {item['price']},
                                    {item['quantity']}
                                )
                            """).collect()
                        
                        # Add tax as a separate item
                        session.sql(f"""
                            INSERT INTO quote_items (QUOTEID, ITEM_TYPE, ITEM_ID, PRICE, QUANTITY)
                            VALUES (
                                '{quote_id}',
                                'TAX',
                                'Sales Tax ({install_tax_rate}%)',
                                {tax},
                                1
                            )
                        """).collect()
                        
                        # Update customer record
                        session.sql(f"""
                            UPDATE customers 
                            SET LAST_QUOTE_ID = '{quote_id}',
                                LAST_QUOTE_DATE = '{datetime.now()}'
                            WHERE CUSTOMERID = '{selected_customer_id}'
                        """).collect()
                        
                        st.success("Installation quote created successfully!")
                        
                        # Generate the quote document
                        quote_doc = f"""
POTOMAC HVAC LLC
(301)825-4447

Invoice / Quote Number: INST-{datetime.now().strftime('%m%d%y')}-{unit_size}
Date: {quote_date.strftime('%B %d, %Y')}

Dear Customer Information:
Name: {customer_info['NAME']}
Phone: {customer_info['PHONE']}
Address: {customer_info['ADDRESS']}

Service Requested: New {selected_brand} {unit_size} Ton HVAC Installation

Installation Details:
{install_description}

Itemized Quote:
{chr(10).join(f"- {item['type']}: {item['description']} ({item['quantity']} x ${item['price']:.2f}) = ${item['price'] * item['quantity']:.2f}" for item in st.session_state.quote_items)}
- Tax: ${tax:.2f}
----------------------------------
Total: ${total:.2f}

Payment:
To initiate the installation process, we require a deposit of half the total price. 
The installation will commence within five business days of receiving the deposit. 
The remaining balance is due upon completion of the installation.

We accept payment by check, cash, money order, Zelle, Venmo, and credit card (with a 3% fee).

Warranty:
We believe in the quality of our work and the products we use. 
All units come with a 10-year part warranty by the manufacturer after registration. 
Please note that labor costs are not included in this warranty. 
However, our installation warranty is for a lifetime, covering any problems related to the installation.

We truly appreciate your business and the trust you've placed in us.

Potomac HVAC LLC
(301)825-4447
"""
                        
                        # Store the quote document for download/email
                        st.session_state.current_quote = quote_doc
                        st.session_state.current_quote_id = quote_id
                        st.session_state.current_customer_email = customer_info['EMAIL']
                        st.session_state.quote_items = []  # Clear the quote items after finalizing
                        
                    except Exception as e:
                        st.error(f"Error creating installation quote: {str(e)}")
        else:
            st.info("No items added to quote yet. Add unit or materials above.")

    # Document actions section (appears after quote creation)
    if 'current_quote' in st.session_state:
        st.subheader("Quote Actions")
        
        # Download button
        st.download_button(
            label="Download Quote Document",
            data=st.session_state.current_quote,
            file_name=f"quote_{st.session_state.current_quote_id}.txt",
            mime="text/plain"
        )
        
        # Email button (if customer has email)
        if st.session_state.current_customer_email:
            if st.button("Email Quote to Customer"):
                try:
                    # In a real implementation, you would integrate with an email service here
                    # This is just a placeholder
                    st.success(f"Quote emailed to {st.session_state.current_customer_email}")
                    
                    # Update quote status to 'sent'
                    session.sql(f"""
                        UPDATE quotes 
                        SET STATUS = 'sent',
                            SENT_DATE = CURRENT_TIMESTAMP(),
                            SENT_METHOD = 'email'
                        WHERE QUOTEID = '{st.session_state.current_quote_id}'
                    """).collect()
                    
                except Exception as e:
                    st.error(f"Error sending email: {str(e)}")
        else:
            st.warning("Customer doesn't have an email address on file")
        
        # Mark as sent manually
        if st.button("Mark as Sent (Other Method)"):
            try:
                session.sql(f"""
                    UPDATE quotes 
                    SET STATUS = 'sent',
                        SENT_DATE = CURRENT_TIMESTAMP(),
                        SENT_METHOD = 'other'
                    WHERE QUOTEID = '{st.session_state.current_quote_id}'
                """).collect()
                st.success("Quote marked as sent")
            except Exception as e:
                st.error(f"Error updating quote status: {str(e)}")

             

#######################################################################
#######################################################################
#######################################################################
#######################################################################

# Invoices
def invoices():
    st.subheader("🧾 coming soon")
    session = get_session()


#######################################################################
# Payments
def payments():
    st.subheader("💳 Payments , coming soon")
    session = get_session()

#######################################################################

# Reports
def reports():
    st.subheader("📈 Reports coming soon")
    session = get_session()

#######################################################################
# Analytics
# Analytics Page
def analytics():
    st.subheader("📈 Analytics is coming soon")
    
#######################################################################

# Admin Tab: Manage All Tables

def admin_tables():
    st.subheader("🛠 Admin Tables")
    session = get_session()
    
    # List of all tables including the schedule table
    tables = [
        "employees", "customers", "appointments", "quotes", "jobs", 
        "invoices", "roles", "employee_roles", "payment_methods", 
        "payments", "allservices", "equipment", "materials", "employee_schedules"
    ]
    
    # Select table to manage
    selected_table = st.selectbox("Select Table", tables)
    
    # Special handling for employee_schedules table
    if selected_table == "employee_schedules":
        st.subheader("📅 Employee Schedule Management")
        
        # Date range selection - default to current week
        today = datetime.now().date()
        start_of_week = today - timedelta(days=today.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Week Starting", value=start_of_week)
        with col2:
            end_date = st.date_input("Week Ending", value=end_of_week)
        
        # Get all employees for dropdown
        employees = session.sql("SELECT employeeid, ename FROM employees ORDER BY ename").collect()
        employee_options = {e['EMPLOYEEID']: e['ENAME'] for e in employees}
        
        # Get all schedules for the selected week
        schedules = session.sql(f"""
            SELECT s.*, e.ename 
            FROM employee_schedules s
            JOIN employees e ON s.employeeid = e.employeeid
            WHERE s.schedule_date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY s.schedule_date, s.start_time
        """).collect()
        
        # Create calendar view - days as columns, hours as rows
        st.subheader(f"📅 Weekly Schedule: {start_date.strftime('%m/%d')} - {end_date.strftime('%m/%d')}")
        
        # Define time slots (8AM to 6PM in 2-hour increments)
        time_slots = [
            ("8:00-10:00", time(8, 0), time(10, 0)),
            ("10:00-12:00", time(10, 0), time(12, 0)),
            ("12:00-14:00", time(12, 0), time(14, 0)),
            ("14:00-16:00", time(14, 0), time(16, 0)),
            ("16:00-18:00", time(16, 0), time(18, 0))
        ]
        
        # Get all days in the week
        days = [(start_date + timedelta(days=i)).strftime("%a %m/%d") for i in range(7)]
        day_dates = [start_date + timedelta(days=i) for i in range(7)]
        
        # Create custom CSS for employee boxes
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
        
        # Create HTML table
        table_html = """
        <table class="schedule-table">
            <tr>
                <th>Time Slot</th>
        """
        
        # Add day headers
        for day in days:
            table_html += f"<th>{day}</th>"
        table_html += "</tr>"
        
        # Add time slots and employee boxes
        for slot_name, slot_start, slot_end in time_slots:
            table_html += f"<tr><td class='time-col'>{slot_name}</td>"
            
            for day_date in day_dates:
                # Find schedules for this day and time slot
                day_schedules = []
                for s in schedules:
                    if s['SCHEDULE_DATE'] == day_date:
                        s_start = s['START_TIME']
                        s_end = s['END_TIME']
                        # Check if schedule overlaps with time slot
                        if (s_start < slot_end) and (s_end > slot_start):
                            day_schedules.append(s['ENAME'])
                
                # Create cell with employee boxes
                table_html += "<td>"
                for name in day_schedules:
                    table_html += f"<div class='employee-box'>{name}</div>"
                table_html += "</td>"
            
            table_html += "</tr>"
        
        table_html += "</table>"
        
        # Display the table
        st.markdown(table_html, unsafe_allow_html=True)
        
        # Schedule management form
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
                    # Validate time range
                    if start_time >= end_time:
                        st.error("End time must be after start time!")
                    else:
                        # Check for existing schedules that conflict
                        existing = session.sql(f"""
                            SELECT * FROM employee_schedules
                            WHERE employeeid = '{employee}'
                            AND schedule_date = '{schedule_date}'
                            AND (
                                (start_time < '{end_time}' AND end_time > '{start_time}')
                            )
                        """).collect()
                        
                        if existing:
                            st.error("This employee already has a schedule during this time period!")
                        else:
                            try:
                                # Check for duplicate schedule
                                duplicate = session.sql(f"""
                                    SELECT * FROM employee_schedules
                                    WHERE employeeid = '{employee}'
                                    AND schedule_date = '{schedule_date}'
                                    AND start_time = '{start_time}'
                                    AND end_time = '{end_time}'
                                """).collect()
                                
                                if duplicate:
                                    st.error("This exact schedule already exists for this employee!")
                                else:
                                    schedule_id = f"SCH{datetime.now().timestamp()}"
                                    session.sql(f"""
                                        INSERT INTO employee_schedules (
                                            scheduleid, employeeid, schedule_date, 
                                            start_time, end_time, notes
                                        ) VALUES (
                                            '{schedule_id}',
                                            '{employee}',
                                            '{schedule_date}',
                                            '{start_time}',
                                            '{end_time}',
                                            '{notes.replace("'", "''")}'
                                        )
                                    """).collect()
                                    st.success("Schedule added successfully!")
                                    st.rerun()
                            except Exception as e:
                                st.error(f"Error saving schedule: {str(e)}")
        
        # Delete schedule option
        with st.expander("🗑️ Delete Schedules"):
            if schedules:
                # Group schedules by employee and date for better organization
                schedule_groups = {}
                for s in schedules:
                    key = f"{s['ENAME']} - {s['SCHEDULE_DATE']}"
                    if key not in schedule_groups:
                        schedule_groups[key] = []
                    schedule_groups[key].append(s)
                
                # Create select box with grouped options
                selected_group = st.selectbox(
                    "Select employee and date",
                    options=list(schedule_groups.keys())
                )
                
                # Show schedules for selected employee/date
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
                        session.sql(f"""
                            DELETE FROM employee_schedules
                            WHERE scheduleid = '{schedule_id}'
                        """).collect()
                        st.success("Schedule deleted!")
                        st.rerun()
            else:
                st.info("No schedules to delete for selected week")
    
    else:
        # Standard table management for all other tables
        st.subheader(f"Manage {selected_table.capitalize()}")
        
        # Fetch data from selected table
        table_data = session.table(selected_table).collect()
        if table_data:
            st.dataframe(table_data)
        
        # Add new record
        with st.expander("➕ Add New Record"):
            with st.form(f"add_{selected_table}_form"):
                # Dynamically create input fields based on table columns
                columns = session.table(selected_table).columns
                input_values = {}
                for col in columns:
                    if col.lower().endswith("id"):  # Skip ID fields (auto-generated)
                        continue
                    input_values[col] = st.text_input(f"{col}")
                
                if st.form_submit_button("Add Record"):
                    try:
                        # Generate ID if not provided
                        if "id" in [c.lower() for c in columns]:
                            input_values[columns[0]] = f"{selected_table.upper()}_{datetime.now().timestamp()}"
                        
                        # Build SQL query
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
        
        # Edit/Delete record
        with st.expander("✏️ Edit/Delete Record"):
            if table_data:
                selected_record = st.selectbox(
                    f"Select Record to Edit/Delete",
                    options=[row[columns[0]] for row in table_data]
                )
                
                if selected_record:
                    record_data = [row for row in table_data if row[columns[0]] == selected_record][0]
                    
                    with st.form(f"edit_{selected_table}_form"):
                        # Dynamically create input fields for editing
                        edit_values = {}
                        for col in columns:
                            if col.lower().endswith("id"):  # Skip ID fields (read-only)
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
 
                                     
  
#######################################################################
# Main app function
   

def main_app():
    st.sidebar.title(f"Welcome {st.session_state.user_name}")

    # Available tabs based on roles
    available_tabs = set()
    for role in st.session_state.roles:
        available_tabs.update(ROLE_ACCESS.get(role.lower(), []))

    # Add the "profile" tab for all employees
    available_tabs.add("profile")

    # Define tab order
    tab_order = ['Home', 'profile', 'customers', 'appointments', 'quotes', 
                'invoices', 'payments', 'reports', 'analytics', 'admin_tables', 'equipment']
    
    available_tabs = [tab for tab in tab_order if tab in available_tabs]

    # Sidebar navigation
    selected_tab = st.sidebar.selectbox("Navigation", available_tabs)

    if selected_tab == 'Home':
        Home()
    elif selected_tab == 'profile':
        profile_page()    
    elif selected_tab == 'customers':
        customer_management()
    elif selected_tab == 'equipment':
        equipment_management()    
    elif selected_tab == 'appointments':
        appointments()
    elif selected_tab == 'quotes':
        quotes()
    elif selected_tab == 'invoices':
        invoices()
    elif selected_tab == 'payments':
        payments()
    elif selected_tab == 'reports':
        reports()
    elif selected_tab == 'analytics':
        analytics()
    elif selected_tab == 'admin_tables':
        admin_tables()  # Now this is defined before being called

    # Logout button
    if st.sidebar.button("Logout"):
        st.session_state.clear()
        st.rerun()

# Main app flow
if __name__ == '__main__':
    query_params = st.query_params
    if 'reset_token' in query_params:
        reset_password(query_params['reset_token'])
    elif not st.session_state.get('logged_in'):
        login_page()
    else:
        main_app() 
