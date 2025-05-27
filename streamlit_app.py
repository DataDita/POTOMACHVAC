# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Get the current credentials
session = get_active_session()



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


from snowflake.snowpark.functions import col
cnx=st.connection("snowflake")
session = cnx.session()
from snowflake.snowpark import Session
from datetime import datetime, timedelta
import re
import uuid
import hashlib
import base64
from PIL import Image
import io
# Add this helper function with your imports
from PIL import Image, ImageOps
import io
# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests


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
#######################################################################
######################################################################
#############HOME#####################################################

def Home():
    # Establish connection to Snowflake database using existing session
    session = get_session()
    
    # Initialize variables for time tracking with default values
    selected_date = datetime.now().date()  # Set default date to today
    manual_time = False  # Flag for manual time entry mode
    manual_clock_in = None  # Stores manually entered clock-in time
    manual_clock_out = None  # Stores manually entered clock-out time
    manual_break_start = None  # Stores manually entered break start time
    manual_break_end = None  # Stores manually entered break end time
    is_clocked_in = False  # Tracks if employee is currently clocked in
    is_on_break = False  # Tracks if employee is currently on break
    time_entry = []  # Stores time clock entries from database
    break_entry = []  # Stores break entries from database

    # --- UI Layout ---
    # Create main page title
    st.title("Home")
    
    # Create header section with profile information
    col1, col2 = st.columns([4, 1])  # Split layout into two columns (4:1 ratio)
    with col1:  # Left column for user info
        # Display welcome message with user's name
        st.subheader(f"Welcome, {st.session_state.user_name}!")
        # Show user's roles in smaller text
        st.caption(f"Role: {', '.join(st.session_state.roles)}")

    with col2:  # Right column for profile picture
        try:  # Handle potential errors in profile picture loading
            # Query database for most recent profile picture
            pic_data = session.sql(f"""
                SELECT PICTURE_DATA_TEXT FROM EMPLOYEE_PICTURES
                WHERE EMPLOYEEID = '{st.session_state.user_id}'
                ORDER BY UPLOADED_AT DESC LIMIT 1
            """).collect()
            
            if pic_data and pic_data[0]['PICTURE_DATA_TEXT']:
                # Decode base64 image data and display
                img = Image.open(io.BytesIO(base64.b64decode(pic_data[0]['PICTURE_DATA_TEXT'])))
                st.image(img, width=100)  # Show image at 100px width
            else:
                # Display placeholder gray image if no picture exists
                st.image(Image.new('RGB', (100,100), color='gray'), width=100)
        except Exception as e:  # Catch and display any image loading errors
            st.error(f"Couldn't load profile picture: {str(e)}")

    # --- Time Tracking Section ---
    # Create section header for time tracking
    st.subheader("Time Tracking")
    # Add toggle switch for manual time entry mode
    manual_time = st.toggle("Manual Time Entry", help="Enable to enter times manually")
    
    # Generate time options for manual entry dropdowns
    time_options = []  # Store tuples of (display time, time object)
    for hour in range(7, 23):  # Hours from 7 AM to 10 PM (23 in 24h format)
        for minute in [0, 15, 30, 45]:  # 15-minute intervals
            time_obj = time(hour, minute)  # Create time object
            # Format as 12-hour time with AM/PM and add to options
            time_options.append((time_obj.strftime("%I:%M %p"), time_obj))
    
    if manual_time:  # Manual time entry mode
        with st.form("manual_time_form"):  # Create form container
            st.warning("Manual Entry Mode - For correcting missed punches")
            
            # Date selection for manual entry
            selected_date = st.date_input(
                "Entry Date",
                value=datetime.now().date(),  # Default to today
                min_value=datetime.now().date() - timedelta(days=30),  # Allow past 30 days
                max_value=datetime.now().date()  # Don't allow future dates
            )
            
            # Create two columns for clock in/out times
            cols = st.columns(2)
            with cols[0]:  # Clock-in column
                selected_clock_in = st.selectbox(
                    "Clock In Time",
                    options=[t[0] for t in time_options],  # Display times
                    index=0,  # Default to first option
                    key="clock_in_select"
                )
                # Get corresponding time object from selection
                manual_clock_in = next(t[1] for t in time_options if t[0] == selected_clock_in)
            
            with cols[1]:  # Clock-out column
                selected_clock_out = st.selectbox(
                    "Clock Out Time",
                    options=["Not clocked out yet"] + [t[0] for t in time_options],
                    index=0,
                    key="clock_out_select"
                )
                # Handle "not clocked out" selection
                manual_clock_out = next((t[1] for t in time_options if t[0] == selected_clock_out), None)
            
            # Create two columns for break times
            cols = st.columns(2)
            with cols[0]:  # Break start column
                selected_break_start = st.selectbox(
                    "Break Start",
                    options=["No break"] + [t[0] for t in time_options],
                    index=0,
                    key="break_start_select"
                )
                manual_break_start = next((t[1] for t in time_options if t[0] == selected_break_start), None)
            
            with cols[1]:  # Break end column
                selected_break_end = st.selectbox(
                    "Break End",
                    options=["Break not ended"] + [t[0] for t in time_options],
                    index=0,
                    key="break_end_select"
                )
                manual_break_end = next((t[1] for t in time_options if t[0] == selected_break_end), None)
            
            # Form submission handler
            if st.form_submit_button("Save Manual Entry"):
                if not manual_clock_in:  # Validate required field
                    st.error("Clock in time is required")
                else:
                    try:
                        # Convert times to datetime objects with selected date
                        clock_in_dt = datetime.combine(selected_date, manual_clock_in)
                        clock_out_dt = datetime.combine(selected_date, manual_clock_out) if manual_clock_out else None
                        
                        # Check for existing time entry
                        existing = session.sql(f"""
                            SELECT ENTRYID FROM employee_time_entries
                            WHERE EMPLOYEEID = '{st.session_state.user_id}'
                            AND ENTRY_DATE = '{selected_date}'
                            LIMIT 1
                        """).collect()
                        
                        if existing:  # Update existing entry
                            session.sql(f"""
                                UPDATE employee_time_entries
                                SET CLOCK_IN = '{clock_in_dt}',
                                    CLOCK_OUT = {'NULL' if clock_out_dt is None else f"'{clock_out_dt}'"}
                                WHERE ENTRYID = '{existing[0]['ENTRYID']}'
                            """).collect()
                        else:  # Create new entry
                            entry_id = f"ENTRY{datetime.now().timestamp()}"  # Generate unique ID
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
                        
                        # Handle break entries if provided
                        if manual_break_start and manual_break_end:
                            # Convert break times to datetime objects
                            break_start_dt = datetime.combine(selected_date, manual_break_start)
                            break_end_dt = datetime.combine(selected_date, manual_break_end)
                            
                            # Check for existing break entry
                            existing_break = session.sql(f"""
                                SELECT BREAKID FROM employee_break_entries
                                WHERE EMPLOYEEID = '{st.session_state.user_id}'
                                AND ENTRY_DATE = '{selected_date}'
                                LIMIT 1
                            """).collect()
                            
                            if existing_break:  # Update existing break
                                session.sql(f"""
                                    UPDATE employee_break_entries
                                    SET BREAK_START = '{break_start_dt}',
                                        BREAK_END = '{break_end_dt}'
                                    WHERE BREAKID = '{existing_break[0]['BREAKID']}'
                                """).collect()
                            else:  # Create new break entry
                                break_id = f"BREAK{datetime.now().timestamp()}"
                                session.sql(f"""
                                    INSERT INTO employee_break_entries
                                    (BREAKID, EMPLOYEEID, BREAK_START, BREAK_END, ENTRY_DATE)
                                    VALUES (
                                        '{break_id}',
                                        '{st.session_state.user_id}',
                                        '{break_start_dt}',
                                        '{break_end_dt}',
                                        '{selected_date}'
                                    )
                                """).collect()
                        
                        st.success("Time entry saved successfully!")
                        st.rerun()  # Refresh page to show changes
                    except Exception as e:  # Handle database errors
                        st.error(f"Error saving time entry: {str(e)}")
    
    else:  # Automatic time tracking mode
        # Set date to today for automatic tracking
        selected_date = datetime.now().date()
        # Get current time entries from database
        time_entry = session.sql(f"""
            SELECT * FROM employee_time_entries
            WHERE EMPLOYEEID = '{st.session_state.user_id}'
            AND ENTRY_DATE = '{selected_date}'
            ORDER BY CLOCK_IN DESC
            LIMIT 1
        """).collect()
        
        # Get current break entries from database
        break_entry = session.sql(f"""
            SELECT * FROM employee_break_entries
            WHERE EMPLOYEEID = '{st.session_state.user_id}'
            AND ENTRY_DATE = '{selected_date}'
            ORDER BY BREAK_START DESC
            LIMIT 1
        """).collect()
        
        # Determine current clock status
        is_clocked_in = len(time_entry) > 0 and time_entry[0]['CLOCK_OUT'] is None
        # Determine current break status
        is_on_break = len(break_entry) > 0 and break_entry[0]['BREAK_END'] is None
        
        # Create time tracking buttons
        cols = st.columns(2)  # Split into two columns
        with cols[0]:  # Clock In button
            if st.button("🟢 Clock In", disabled=is_clocked_in):
                # Insert new time entry with current timestamp
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
                st.rerun()  # Refresh to update status
        
        with cols[1]:  # Clock Out button
            if st.button("🔴 Clock Out", disabled=not is_clocked_in or is_on_break):
                # Update most recent entry with clock-out time
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
        with cols[0]:  # Start Break button
            if st.button("🟡 Start Break", disabled=not is_clocked_in or is_on_break):
                # Insert new break entry with start time
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
        
        with cols[1]:  # End Break button
            if st.button("🟢 End Break", disabled=not is_on_break):
                # Update most recent break with end time
                session.sql(f"""
                    UPDATE employee_break_entries
                    SET BREAK_END = CURRENT_TIMESTAMP()
                    WHERE EMPLOYEEID = '{st.session_state.user_id}'
                    AND ENTRY_DATE = '{selected_date}'
                    AND BREAK_END IS NULL
                """).collect()
                st.rerun()

    # Refresh time data after potential updates
    time_entry = session.sql(f"""
        SELECT * FROM employee_time_entries
        WHERE EMPLOYEEID = '{st.session_state.user_id}'
        AND ENTRY_DATE = '{selected_date}'
        ORDER BY CLOCK_IN DESC
        LIMIT 1
    """).collect()
    
    # Refresh break data after potential updates
    break_entry = session.sql(f"""
        SELECT * FROM employee_break_entries
        WHERE EMPLOYEEID = '{st.session_state.user_id}'
        AND ENTRY_DATE = '{selected_date}'
        ORDER BY BREAK_START DESC
        LIMIT 1
    """).collect()
    
    # Create two columns for status displays
    cols = st.columns(2)
    with cols[0]:  # Left status column
        # Clock status display
        if time_entry:
            if time_entry[0]['CLOCK_OUT'] is None:  # Currently clocked in
                st.write(f"**Clocked In:**  {time_entry[0]['CLOCK_IN'].strftime('%I:%M %p')}")
            else:  # Clocked out
                st.info("🔴 Clocked Out")
                st.write(f"**Worked:** {time_entry[0]['CLOCK_IN'].strftime('%I:%M %p')} to {time_entry[0]['CLOCK_OUT'].strftime('%I:%M %p')}")

        # Break status display (only shown if clocked in)
        if time_entry and time_entry[0]['CLOCK_OUT'] is None:
            if break_entry:
                if break_entry[0]['BREAK_END'] is None:  # Currently on break
                    st.error("🟡 Currently On Break")
                    st.write(f"**Since:** {break_entry[0]['BREAK_START'].strftime('%I:%M %p')}")
                else:  # Break completed
                    st.write(f"**Break:** {break_entry[0]['BREAK_START'].strftime('%I:%M %p')} to {break_entry[0]['BREAK_END'].strftime('%I:%M %p')}")
            else:  # No break taken
                st.success("✅ Available for Break")

    # Calculate and display total worked time
    time_entries = session.sql(f"""
        SELECT CLOCK_IN, CLOCK_OUT 
        FROM employee_time_entries
        WHERE EMPLOYEEID = '{st.session_state.user_id}'
        AND ENTRY_DATE = '{selected_date}'
        ORDER BY CLOCK_IN
    """).collect()
    
    # Get all break entries for the day
    break_entries = session.sql(f"""
        SELECT BREAK_START, BREAK_END 
        FROM employee_break_entries
        WHERE EMPLOYEEID = '{st.session_state.user_id}'
        AND ENTRY_DATE = '{selected_date}'
        ORDER BY BREAK_START
    """).collect()

    if time_entries:
        # Calculate total worked seconds
        total_seconds = 0
        for entry in time_entries:
            if entry['CLOCK_OUT']:
                # Add difference between clock out and in times
                total_seconds += (entry['CLOCK_OUT'] - entry['CLOCK_IN']).total_seconds()
            else:
                # Add time since clock in if still clocked in
                total_seconds += (datetime.now() - entry['CLOCK_IN']).total_seconds()
        
        # Calculate total break time in seconds
        break_seconds = 0
        for entry in break_entries:
            if entry['BREAK_END']:
                break_seconds += (entry['BREAK_END'] - entry['BREAK_START']).total_seconds()
        
        # Calculate net worked time
        net_seconds = total_seconds - break_seconds
        # Convert seconds to hours and minutes
        hours = int(net_seconds // 3600)
        minutes = int((net_seconds % 3600) // 60)
        
        # Display time with proper pluralization
        time_str = f"{hours} hour{'s' if hours != 1 else ''} {minutes} minute{'s' if minutes != 1 else ''}"
        st.markdown("---")  # Horizontal line
        st.metric("Total Worked Today", time_str)  # Display metric
        st.markdown("---")

    # --- Upcoming Appointments Section ---
    st.header("📅 Upcoming Appointments")  # Section header

    # Query database for appointments
    appointments = session.sql(f"""
        SELECT 
            a.appointmentid,
            a.service_type,
            a.scheduled_time,
            a.sta_tus,
            c.name AS customer_name,
            c.address,
            c.unit,
            c.city,
            c.state,
            c.zipcode,
            c.has_lock_box,
            c.lock_box_code,
            c.has_safety_alarm,
            c.safety_alarm,
            c.entrance_note,
            c.note,
            c.unit_location,
            c.accessibility_level,
            c.phone,
            c.email
        FROM appointments a
        JOIN customers c ON a.customerid = c.customerid
        WHERE a.technicianid = '{st.session_state.user_id}'
        AND a.scheduled_time BETWEEN CURRENT_TIMESTAMP() 
            AND DATEADD('day', 7, CURRENT_TIMESTAMP())
        ORDER BY a.scheduled_time
    """).collect()

    if not appointments:  # Handle no appointments case
        st.info("No upcoming appointments in the next 7 days")
        return

    # Display each appointment in a row
    for appt in appointments:
        # Create 4-column layout per appointment
        col1, col2, col3, col4 = st.columns([1,1,3,1])
        
        with col1:  # Service type column
            st.markdown(f"**{appt['SERVICE_TYPE'].capitalize()}**")  # Capitalized service name
        
        with col2:  # Time column
            dt = appt['SCHEDULED_TIME']
            # Format date/time in compact form
            st.write(f"{dt.strftime('%a %m/%d')}\n{dt.strftime('%I:%M %p')}")
        
        with col3:  # Customer info column
            full_address = f"{appt['ADDRESS']}, {appt['CITY']}, {appt['STATE']} {appt['ZIPCODE']}"
            # Create Google Maps link
            maps_url = f"https://www.google.com/maps/search/?api=1&query={full_address.replace(' ', '+')}"
            
            # Expandable section for detailed customer info
            with st.expander(f"**{appt['CUSTOMER_NAME']}** - {appt['ADDRESS']}, {appt['CITY']}"):
                # Clickable address link
                st.markdown(f"**Complete Address:** [📌 {full_address}]({maps_url})")
                # Display all customer details
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

        with col4:  # Status management column
            current_status = appt['STA_TUS'].lower()  # Get lowercase status
            # Color coding for status badges
            status_colors = {
                'scheduled': '#4a4a4a',  # Dark gray
                'accepted': '#2e7d32',   # Green
                'declined': '#c62828',    # Red
                'arrived': '#1565c0'      # Blue
            }
            
            # Create styled status badge
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
            
            # Status transition buttons
            if current_status == 'scheduled':
                # Accept appointment button
                if st.button("✅ Accept", key=f"accept_{appt['APPOINTMENTID']}"):
                    session.sql(f"""
                        UPDATE appointments
                        SET STA_TUS = 'accepted'
                        WHERE APPOINTMENTID = '{appt['APPOINTMENTID']}'
                    """).collect()
                    st.rerun()
                
                # Decline appointment button
                if st.button("❌ Decline", key=f"decline_{appt['APPOINTMENTID']}"):
                    session.sql(f"""
                        UPDATE appointments
                        SET STA_TUS = 'declined'
                        WHERE APPOINTMENTID = '{appt['APPOINTMENTID']}'
                    """).collect()
                    st.rerun()
            
            elif current_status == 'accepted':
                # Mark as arrived button
                if st.button("📍 I'm Here", key=f"arrived_{appt['APPOINTMENTID']}"):
                    session.sql(f"""
                        UPDATE appointments
                        SET STA_TUS = 'arrived'
                        WHERE APPOINTMENTID = '{appt['APPOINTMENTID']}'
                    """).collect()
                    st.rerun()

        st.markdown("---")  # Divider between appointments
        
        
        
        
#######################################################################
#######################################################################        
#######################################################################
def profile_page():
    # Establish connection to Snowflake database using existing session
    session = get_session()
    
    # --- Profile Header Section ---
    # Create two columns layout (1:4 ratio) for profile picture and info
    col1, col2 = st.columns([1, 4])
    
    with col1:  # Left column for profile picture
        # Query database for most recent profile picture
        pic_data = session.sql(f"""
            SELECT PICTURE_DATA_TEXT FROM EMPLOYEE_PICTURES
            WHERE EMPLOYEEID = '{st.session_state.user_id}'
            ORDER BY UPLOADED_AT DESC LIMIT 1
        """).collect()
        
        # Display profile picture if exists
        if pic_data and pic_data[0]['PICTURE_DATA_TEXT']:
            # Decode base64 image data
            img_data = base64.b64decode(pic_data[0]['PICTURE_DATA_TEXT'])
            # Open image and resize
            img = Image.open(io.BytesIO(img_data))
            img.thumbnail((80, 80), Image.Resampling.LANCZOS)
            # Display resized image
            st.image(img, width=80)
        else:
            # Show placeholder gray image
            st.image(Image.new('RGB', (80, 80), color='lightgray'))
        
        # Create expandable section for picture upload
        with st.expander("🖼"):  # Frame emoji as icon
            # Add hidden tooltip
            st.markdown("<span title='Update profile picture'>", unsafe_allow_html=True)
            # File uploader widget (hidden label)
            uploaded_file = st.file_uploader("", type=["jpg", "jpeg", "png"], key="pic_uploader")
            # Update button with image processing
            if uploaded_file and st.button("Update", key="pic_update"):
                try:
                    # Process uploaded image
                    img = Image.open(uploaded_file)
                    img = ImageOps.fit(img, (500, 500))  # Crop to square
                    # Save to buffer with quality settings
                    buffer = io.BytesIO()
                    img.save(buffer, format="JPEG", quality=90)
                    # Encode image for database storage
                    encoded_image = base64.b64encode(buffer.getvalue()).decode('utf-8')
                    
                    # Insert new picture record
                    session.sql(f"""
                        INSERT INTO EMPLOYEE_PICTURES 
                        (PICTUREID, EMPLOYEEID, PICTURE_DATA_TEXT)
                        VALUES (
                            'PIC{datetime.now().timestamp()}',  

                            '{st.session_state.user_id}',
                          
                            '{encoded_image}'  
                        )
                    """).collect()
                    st.rerun()  # Refresh to show new picture
                except Exception as e:
                    st.error(f"Error: {str(e)}")
            # Close tooltip span
            st.markdown("</span>", unsafe_allow_html=True)

    with col2:  # Right column for profile info
        # Display user name as title
        st.title(f"{st.session_state.user_name}'s Profile")
        # Show employee ID in caption
        st.caption(f"Employee ID: {st.session_state.user_id}")

    # --- Date Range Selector ---
    # Set default date range to current week
    today = datetime.now().date()
    start_of_week = today - timedelta(days=today.weekday())  # Monday
    end_of_week = start_of_week + timedelta(days=6)  # Sunday
    
    # Create expandable date selector
    with st.expander("📅 Date Range", expanded=True):
        col1, col2 = st.columns(2)  # Split into two columns
        with col1:
            # Start date picker with week start default
            start_date = st.date_input("From", value=start_of_week)
        with col2:
            # End date picker with week end default
            end_date = st.date_input("To", value=end_of_week, min_value=start_date)

    # --- Tab Navigation ---
    # Create four tabs for different sections
    tab1, tab2, tab3, tab4 = st.tabs([
        "📅 Schedule", 
        "⏱ Work History", 
        "💰 Earnings", 
        "📝 Appointments"
    ])

    # Tab 1: Schedule
    with tab1:
        # Get employee name from database
        employee_name = session.sql(f"""
            SELECT ename FROM employees
            WHERE employeeid = '{st.session_state.user_id}'
        """).collect()[0]['ENAME']
        
        # Query schedule data
        schedules = session.sql(f"""
            SELECT * FROM employee_schedules
            WHERE employeeid = '{st.session_state.user_id}'
            AND schedule_date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY schedule_date, start_time
        """).collect()
        
        # Custom CSS styling for schedule table
        st.markdown("""
        <style>
            /* Employee time block styling */
            .employee-box {
                display: inline-block;
                background-color: #e6f7ff;
                border-radius: 4px;
                padding: 2px 6px;
                margin: 2px;
                font-size: 12px;
                border: 1px solid #b3e0ff;
            }
            /* Table styling */
            .schedule-table {
                width: 100%;
                border-collapse: collapse;
                font-size: 14px;
            }
            /* Cell styling */
            .schedule-table th, .schedule-table td {
                border: 1px solid #ddd;
                padding: 8px;
                text-align: center;
            }
            /* Header styling */
            .schedule-table th {
                background-color: #f2f2f2;
                font-weight: bold;
            }
            /* Time column styling */
            .time-col {
                background-color: #f9f9f9;
                font-weight: bold;
            }
        </style>
        """, unsafe_allow_html=True)
        
        # Define time slots for schedule grid
        time_slots = [
            ("8:00-10:00", time(8, 0), time(10, 0)),
            ("10:00-12:00", time(10, 0), time(12, 0)),
            ("12:00-14:00", time(12, 0), time(14, 0)),
            ("14:00-16:00", time(14, 0), time(16, 0)),
            ("16:00-18:00", time(16, 0), time(18, 0))
        ]
        
        # Generate date labels for columns
        days = [(start_date + timedelta(days=i)).strftime("%a %m/%d") 
               for i in range((end_date - start_date).days + 1)]
        # Generate actual date objects
        day_dates = [start_date + timedelta(days=i) 
                    for i in range((end_date - start_date).days + 1)]
        
        # Build HTML table structure
        table_html = """
        <table class="schedule-table">
            <tr>
                <th>Time Slot</th>
        """
        # Add day headers
        for day in days:
            table_html += f"<th>{day}</th>"
        table_html += "</tr>"
        
        # Populate table rows
        for slot_name, slot_start, slot_end in time_slots:
            table_html += f"<tr><td class='time-col'>{slot_name}</td>"
            
            # Check each day for scheduled time
            for day_date in day_dates:
                scheduled = False
                for s in schedules:
                    if s['SCHEDULE_DATE'] == day_date:
                        # Get schedule times
                        s_start = s['START_TIME']
                        s_end = s['END_TIME']
                        # Check for time overlap
                        if (s_start < slot_end) and (s_end > slot_start):
                            scheduled = True
                            break
                
                # Add cell content
                table_html += "<td>"
                if scheduled:
                    table_html += f"<div class='employee-box'>{employee_name}</div>"
                table_html += "</td>"
            
            table_html += "</tr>"
        
        table_html += "</table>"
        # Render HTML table
        st.markdown(table_html, unsafe_allow_html=True)

    # Tab 2: Work History
    with tab2:
        # Query time entries from database
        time_entries = session.sql(f"""
            SELECT 
                ENTRY_DATE,
                CLOCK_IN,
                CLOCK_OUT,
                TIMEDIFF('MINUTE', CLOCK_IN, CLOCK_OUT)/60.0 as hours_worked
            FROM employee_time_entries
            WHERE EMPLOYEEID = '{st.session_state.user_id}'
            AND ENTRY_DATE BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY ENTRY_DATE DESC, CLOCK_IN DESC
        """).collect()
        
        if time_entries:
            # Calculate total hours
            total_hours = sum(entry['HOURS_WORKED'] or 0 for entry in time_entries)
            
            # Create formatted dataframe
            st.dataframe(
                pd.DataFrame([{
                    "Date": e['ENTRY_DATE'].strftime('%Y-%m-%d'),
                    "Clock In": e['CLOCK_IN'].strftime('%I:%M %p') if e['CLOCK_IN'] else "-",
                    "Clock Out": e['CLOCK_OUT'].strftime('%I:%M %p') if e['CLOCK_OUT'] else "-",
                    "Hours": f"{e['HOURS_WORKED']:.2f}" if e['HOURS_WORKED'] else "-"
                } for e in time_entries]),
                hide_index=True,  # Hide pandas index
                use_container_width=True  # Full-width display
            )
            
            # Display total hours metric
            st.metric("Total Hours Worked", f"{total_hours:.2f}")
        else:
            st.info("No work history for selected period")

    # Tab 3: Earnings
    with tab3:
        # Get employee hourly rate
        emp_rate = session.sql(f"""
            SELECT hourlyrate FROM employees
            WHERE employeeid = '{st.session_state.user_id}'
        """).collect()[0]['HOURLYRATE']
        
        # Query earnings data
        earnings = session.sql(f"""
            SELECT 
                ENTRY_DATE,
                SUM(TIMEDIFF('MINUTE', CLOCK_IN, CLOCK_OUT)/60.0) as hours_worked
            FROM employee_time_entries
            WHERE EMPLOYEEID = '{st.session_state.user_id}'
            AND CLOCK_OUT IS NOT NULL  
            AND ENTRY_DATE BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY ENTRY_DATE
            ORDER BY ENTRY_DATE DESC
        """).collect()
        
        if earnings:
            # Calculate total earnings
            total_earnings = sum(e['HOURS_WORKED'] * emp_rate for e in earnings)
            
            # Display earnings dataframe
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
            
            # Show total earnings metric
            st.metric("Total Earnings", f"${total_earnings:.2f}")
        else:
            st.info("No earnings data for selected period")

    # Tab 4: Appointments
    with tab4:
        # Query appointments from database
        appointments = session.sql(f"""
            SELECT 
                c.name as customer,
                a.scheduled_time,
                TO_VARCHAR(a.scheduled_time, 'HH12:MI AM') as time,
                a.sta_tus as status,
                a.notes
            FROM appointments a
            JOIN customers c ON a.customerid = c.customerid
            WHERE a.technicianid = '{st.session_state.user_id}'
            AND DATE(a.scheduled_time) BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY a.scheduled_time
        """).collect()
        
        if appointments:
            # Display each appointment in expandable sections
            for appt in appointments:
                # Create expander with formatted title
                with st.expander(f"{appt['SCHEDULED_TIME'].strftime('%a %m/%d')} - {appt['CUSTOMER']} ({appt['TIME']})"):
                    st.write(f"**Status:** {appt['STATUS'].capitalize()}")
                    if appt['NOTES']:
                        st.write(f"**Notes:** {appt['NOTES']}")
        else:
            st.info("No appointments for selected period") 



######################################################################            
#######################################################################            
#######################################################################
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

    # --- Add New Customer Section ---
    with st.expander("➕ Add New Customer", expanded=False):
        form_key = "add_customer_form"
        with st.form(key=form_key, clear_on_submit=True):
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
                
                # How Heard section
                how_heard = st.selectbox(
                    "How did you hear about us?",
                    ["", "Google", "Friend", "Facebook", "Yelp", "Other"],
                    index=0
                )
                friend_name = ""
                if how_heard == "Friend":
                    friend_name = st.text_input("Friend's Name*", value=st.session_state.customer_form_data['friend_name'])
            
            # Lock Box and Safety Alarm section
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
            
            # Equipment Information
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
            
            # Upload pictures
            st.subheader("Unit Pictures")
            uploaded_files = st.file_uploader("Upload pictures of the HVAC unit", 
                                            accept_multiple_files=True, 
                                            type=['jpg', 'jpeg', 'png'])
            
            # Notes section
            note = st.text_area("General Notes", value=st.session_state.customer_form_data['note'])
            entrance_note = st.text_area("Entrance Notes", value=st.session_state.customer_form_data['entrance_note'])

            # Form actions
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
                # Store all values in session state
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
                        lock_box_code_escaped = lock_box_code.replace("'", "''") if has_lock_box == "Yes" and lock_box_code else None
                        safety_alarm_code_escaped = safety_alarm_code.replace("'", "''") if has_safety_alarm == "Yes" and safety_alarm_code else None
                        how_heard_value_escaped = how_heard_value.replace("'", "''") if how_heard_value else None
                        note_escaped = note.replace("'", "''") if note else None
                        entrance_note_escaped = entrance_note.replace("'", "''") if entrance_note else None
                        
                        # Insert customer record with all equipment info
                        insert_query = f"""
                            INSERT INTO customers 
                            (CUSTOMERID, NAME, PHONE, EMAIL, ADDRESS, UNIT, CITY, STATE, ZIPCODE,
                             HAS_LOCK_BOX, LOCK_BOX_CODE, HAS_SAFETY_ALARM, SAFETY_ALARM, HOW_HEARD, 
                             NOTE, ENTRANCE_NOTE, OUTDOOR_UNIT_MODEL, OUTDOOR_UNIT_SERIAL_NUMBER,
                             INDOOR_UNIT_MODEL, INDOOR_UNIT_SERIAL_NUMBER, THERMOSTAT_TYPE,
                             UNIT_LOCATION, ACCESSIBILITY_LEVEL, ACCESSIBILITY_NOTES, OTHER_NOTES)
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
                                '{has_lock_box}',
                                {f"'{lock_box_code_escaped}'" if has_lock_box == "Yes" and lock_box_code else 'NULL'},
                                '{has_safety_alarm}',
                                {f"'{safety_alarm_code_escaped}'" if has_safety_alarm == "Yes" and safety_alarm_code else 'NULL'},
                                {f"'{how_heard_value_escaped}'" if how_heard_value else 'NULL'},
                                {f"'{note_escaped}'" if note else 'NULL'},
                                {f"'{entrance_note_escaped}'" if entrance_note else 'NULL'},
                                '{outdoor_unit_model.replace("'", "''")}',
                                '{outdoor_unit_serial.replace("'", "''")}',
                                '{indoor_unit_model.replace("'", "''")}',
                                '{indoor_unit_serial.replace("'", "''")}',
                                '{thermostat_type.replace("'", "''")}',
                                '{unit_location}',
                                '{accessibility_level}',
                                '',
                                ''
                            )
                        """
                        
                        session.sql(insert_query).collect()
                        
                        # Handle uploaded files
                        if uploaded_files:
                            for uploaded_file in uploaded_files:
                                file_data = uploaded_file.read()
                                encoded_file = base64.b64encode(file_data).decode('utf-8')
                                session.sql(f"""
                                    INSERT INTO customer_documents 
                                    (DOC_ID, CUSTOMERID, DOC_TYPE, DESCRIPTION, DOC_DATA)
                                    VALUES (
                                        'DOC{datetime.now().timestamp()}',
                                        '{customer_id}',
                                        'IMAGE',
                                        'Unit Picture - {uploaded_file.name}',
                                        '{encoded_file}'
                                    )
                                """).collect()
                        
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
                        
                        # Force a rerun to clear the form
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error adding customer: {str(e)}")

    # --- Customer Search and Display Section ---
    

    
    st.subheader("🔍 Search Customers")
    search_term = st.text_input("", placeholder="Search by name, phone, email, or address", key="unified_search")
    
    if search_term:
        customers = session.sql(f"""
            SELECT c.* FROM customers c
            WHERE c.NAME ILIKE '%{search_term}%' 
               OR c.PHONE ILIKE '%{search_term}%'
               OR c.EMAIL ILIKE '%{search_term}%'
               OR c.ADDRESS ILIKE '%{search_term}%'
            ORDER BY c.NAME
        """).collect()
        
        if customers:
            for customer in customers:
                # Convert Row to dictionary safely
                try:
                    customer_dict = customer.as_dict() if hasattr(customer, 'as_dict') else dict(zip(customer._fields, customer))
                except:
                    customer_dict = dict(zip(['CUSTOMERID', 'NAME', 'PHONE', 'EMAIL', 'ADDRESS', 'UNIT', 'CITY', 'STATE', 'ZIPCODE',
                                            'HAS_LOCK_BOX', 'LOCK_BOX_CODE', 'HAS_SAFETY_ALARM', 'SAFETY_ALARM', 'HOW_HEARD',
                                            'CREATED_AT', 'LAST_QUOTE_ID', 'LAST_QUOTE_DATE', 'NOTE', 'ENTRANCE_NOTE',
                                            'OUTDOOR_UNIT_MODEL', 'OUTDOOR_UNIT_SERIAL_NUMBER', 'INDOOR_UNIT_MODEL', 
                                            'INDOOR_UNIT_SERIAL_NUMBER', 'THERMOSTAT_TYPE', 'UNIT_LOCATION', 
                                            'ACCESSIBILITY_LEVEL', 'ACCESSIBILITY_NOTES', 'OTHER_NOTES'], customer))
                
                with st.expander(f"{customer_dict['NAME']} - {customer_dict['PHONE']}"):
                    # Customer Information
                    st.subheader("Customer Information")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write(f"**Customer ID:** {customer_dict['CUSTOMERID']}")
                        st.write(f"**Email:** {customer_dict.get('EMAIL', 'Not provided')}")
                        
                        # Address display with Google Maps link
                        full_address = f"{customer_dict['ADDRESS']}, {customer_dict['CITY']}, {customer_dict['STATE']} {customer_dict['ZIPCODE']}"
                        maps_url = f"https://www.google.com/maps/search/?api=1&query={full_address.replace(' ', '+')}"
                        st.markdown(f"""
                            **Address:** <a href="{maps_url}" target="_blank" style="color: blue; text-decoration: none;">
                            {customer_dict['ADDRESS']}{', ' + customer_dict['UNIT'] if customer_dict.get('UNIT') else ''}<br>
                            {customer_dict['CITY']}, {customer_dict['STATE']} {customer_dict['ZIPCODE']}.
                            </a>
                        """, unsafe_allow_html=True)
                        
                    with col2:
                        # Lock Box Section with input box
                        has_lock_box = customer_dict.get('HAS_LOCK_BOX', 'No')
                        lock_box_code = st.text_input(
                            "Lock Box Code",
                            value=customer_dict.get('LOCK_BOX_CODE', ''),
                            disabled=(has_lock_box != 'Yes'),
                            key=f"lock_box_{customer_dict['CUSTOMERID']}"
                        )
                        if has_lock_box == 'Yes' and not lock_box_code:
                            st.error("Lock box code is required when lock box is present")
                        
                        # Safety Alarm Section with input box
                        has_safety_alarm = customer_dict.get('HAS_SAFETY_ALARM', 'No')
                        safety_alarm_code = st.text_input(
                            "Safety Alarm Code",
                            value=customer_dict.get('SAFETY_ALARM', ''),
                            disabled=(has_safety_alarm != 'Yes'),
                            key=f"safety_alarm_{customer_dict['CUSTOMERID']}"
                        )
                        if has_safety_alarm == 'Yes' and not safety_alarm_code:
                            st.error("Safety alarm code is required when safety alarm is present")
                    
                    st.write(f"**How Heard:** {customer_dict.get('HOW_HEARD', 'Not specified')}")
                    st.write(f"**General Note:** {customer_dict.get('NOTE', 'None')}")
                    st.write(f"**Entrance Note:** {customer_dict.get('ENTRANCE_NOTE', 'None')}")
                    
                    # Show ONLY this customer's appointments
                    st.subheader("📅 Customer Appointments")
                    appointments = session.sql(f"""
                        SELECT a.*, e.ename as technician_name 
                        FROM appointments a
                        JOIN employees e ON a.technicianid = e.employeeid
                        WHERE a.customerid = '{customer_dict['CUSTOMERID']}'
                        ORDER BY a.scheduled_time DESC
                    """).collect()
                    
                    if appointments:
                        for appt in appointments:
                            try:
                                appt_dict = appt.as_dict() if hasattr(appt, 'as_dict') else dict(zip(appt._fields, appt))
                                with st.expander(f"{appt_dict['SCHEDULED_TIME'].strftime('%Y-%m-%d %I:%M %p')} - {appt_dict['SERVICE_TYPE']} ({appt_dict['STA_TUS']})"):
                                    st.write(f"**Technician:** {appt_dict['TECHNICIAN_NAME']}")
                                    st.write(f"**Service Type:** {appt_dict['SERVICE_TYPE']}")
                                    st.write(f"**Status:** {appt_dict['STA_TUS']}")
                                    if appt_dict.get('NOTES'):
                                        st.write(f"**Notes:** {appt_dict['NOTES']}")
                            except:
                                pass
                    else:
                        st.info("No appointments scheduled for this customer")
                    
                    # Equipment Information
                    st.subheader("Equipment Information")
                    # ... [rest of equipment info display remains the same] ...
                    
                    # Action buttons
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


    # --- Add Unit Picture Section ---
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
                        session.sql(f"""
                            INSERT INTO customer_documents 
                            (DOC_ID, CUSTOMERID, DOC_TYPE, DESCRIPTION, DOC_DATA)
                            VALUES (
                                'DOC{datetime.now().timestamp()}',
                                '{st.session_state['add_picture_customer']}',
                                'IMAGE',
                                '{description.replace("'", "''")}',
                                '{encoded_file}'
                            )
                        """).collect()
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

    # --- Appointment Scheduling Section ---
    if 'selected_customer_id' in st.session_state and 'selected_customer_name' in st.session_state:
        st.subheader(f"📅 Schedule Appointment for {st.session_state['selected_customer_name']}")
        
        # Service type selection
        request_type = st.selectbox(
            "Select Request Type",
            ["Repair", "Maintenance", "Install", "Estimate"],
            index=0
        )
        
        # Urgent request
        is_urgent = st.radio("Urgent?", ["No", "Yes"], horizontal=True)
        
        # Get qualified technicians
        expertise_map = {
            "Repair": "EX1", 
            "Maintenance": "EX2", 
            "Install": "EX3", 
            "Estimate": "EX4"
        }
        technicians = session.sql(f"""
            SELECT e.employeeid, e.ename 
            FROM employees e
            JOIN employee_expertise ee ON e.employeeid = ee.employeeid
            WHERE ee.expertiseid = '{expertise_map[request_type]}'
        """).collect()
        
        if not technicians:
            st.error("No technicians available for this service type")
        else:
            if request_type == "Install":
                # Full-day booking for installations
                st.subheader("Select Installation Date")
                
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
                        format_func=lambda x: next(t['ENAME'] for t in technicians if t['EMPLOYEEID'] == x))
                    
                    # Select secondary technician (optional)
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
                            # Book primary technician for full day (8AM-5PM)
                            session.sql(f"""
                                INSERT INTO appointments (
                                    appointmentid, customerid, technicianid,
                                    scheduled_time, service_type, notes, sta_tus
                                ) VALUES (
                                    'APT{datetime.now().timestamp()}',
                                    '{st.session_state['selected_customer_id']}',
                                    '{primary_tech}',
                                    '{datetime.combine(date, time(8,0))}',
                                    'Install',
                                    '{notes.replace("'", "''")}',
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
                                        '{st.session_state['selected_customer_id']}',
                                        '{secondary_tech}',
                                        '{datetime.combine(date, time(8,0))}',
                                        'Install-Assist',
                                        '{notes.replace("'", "''")}',
                                        'scheduled'
                                    )
                                """).collect()
                            
                            st.success(f"Installation booked for {date.strftime('%A, %B %d')}!")
                            del st.session_state.selected_install_date
                            del st.session_state['selected_customer_id']
                            del st.session_state['selected_customer_name']
                            st.rerun()
                        
                        except Exception as e:
                            st.error(f"Error: {str(e)}")
            
            else:
                # Standard 2-hour slots for non-installation services
                st.subheader("Select Appointment Time")
                
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
                                        st.rerun()
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
                        format_func=lambda x: next(t['ENAME'] for t in slot['techs'] if t['EMPLOYEEID'] == x))
                    
                    # Secondary technician selection (optional)
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
                                    '{st.session_state['selected_customer_id']}',
                                    '{primary_tech}',
                                    '{slot['datetime']}',
                                    '{request_type}',
                                    '{notes.replace("'", "''")}',
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
                                        '{st.session_state['selected_customer_id']}',
                                        '{secondary_tech}',
                                        '{slot['datetime']}',
                                        '{request_type}-Assist',
                                        '{notes.replace("'", "''")}',
                                        'scheduled'
                                    )
                                """).collect()
                            
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

    # --- Edit Customer Form ---
    if 'edit_customer' in st.session_state and 'customer_to_edit' in st.session_state:
        edit_customer_id = st.session_state['edit_customer']
        customer_to_edit = st.session_state['customer_to_edit']
        
        st.subheader("✏️ Edit Customer")
        form_key = "edit_customer_form"
        with st.form(key=form_key):
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
                
                # Parse how_heard value
                how_heard_value = customer_to_edit.get('HOW_HEARD', '')
                if isinstance(how_heard_value, str) and ":" in how_heard_value:
                    how_heard = how_heard_value.split(":")[0].strip()
                    friend_name = how_heard_value.split(":")[1].strip() if how_heard == "Friend" else ""
                else:
                    how_heard = how_heard_value
                    friend_name = ""
                
                # How Heard section
                how_heard = st.selectbox(
                    "How did you hear about us?",
                    ["", "Google", "Friend", "Facebook", "Yelp", "Other"],
                    index=["", "Google", "Friend", "Facebook", "Yelp", "Other"].index(how_heard) if how_heard in ["", "Google", "Friend", "Facebook", "Yelp", "Other"] else 0
                )
                
                # Show friend name field only if "Friend" is selected
                if how_heard == "Friend":
                    friend_name = st.text_input("Friend's Name*", value=friend_name)
            
            # Lock Box and Safety Alarm section
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
            
            # Equipment Information
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
            
            # Notes section
            note = st.text_area("General Notes", value=customer_to_edit.get('NOTE', ''))
            entrance_note = st.text_area("Entrance Notes", value=customer_to_edit.get('ENTRANCE_NOTE', ''))
            
            # Form actions
            col1, col2 = st.columns(2)
            with col1:
                if st.form_submit_button("💾 Save Changes"):
                    # Validate inputs
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
                            # Prepare how_heard value
                            how_heard_value = how_heard
                            if how_heard == "Friend":
                                how_heard_value = f"Friend: {friend_name}"
                            
                            # Prepare SQL values with proper escaping
                            email_escaped = email.replace("'", "''") if email else None
                            unit_escaped = unit.replace("'", "''") if unit else None
                            lock_box_code_escaped = lock_box_code.replace("'", "''") if has_lock_box == "Yes" and lock_box_code else None
                            safety_alarm_code_escaped = safety_alarm_code.replace("'", "''") if has_safety_alarm == "Yes" and safety_alarm_code else None
                            how_heard_value_escaped = how_heard_value.replace("'", "''") if how_heard_value else None
                            note_escaped = note.replace("'", "''") if note else None
                            entrance_note_escaped = entrance_note.replace("'", "''") if entrance_note else None
                            
                            # Update customer record with all equipment info
                            update_query = f"""
                                UPDATE customers 
                                SET NAME = '{name.replace("'", "''")}',
                                    PHONE = '{phone.replace("'", "''")}',
                                    EMAIL = {f"'{email_escaped}'" if email else 'NULL'},
                                    ADDRESS = '{address.replace("'", "''")}',
                                    UNIT = {f"'{unit_escaped}'" if unit else 'NULL'},
                                    CITY = '{city.replace("'", "''")}',
                                    STATE = '{state}',
                                    ZIPCODE = '{zipcode}',
                                    HAS_LOCK_BOX = '{has_lock_box}',
                                    LOCK_BOX_CODE = {f"'{lock_box_code_escaped}'" if has_lock_box == "Yes" and lock_box_code else 'NULL'},
                                    HAS_SAFETY_ALARM = '{has_safety_alarm}',
                                    SAFETY_ALARM = {f"'{safety_alarm_code_escaped}'" if has_safety_alarm == "Yes" and safety_alarm_code else 'NULL'},
                                    HOW_HEARD = {f"'{how_heard_value_escaped}'" if how_heard_value else 'NULL'},
                                    NOTE = {f"'{note_escaped}'" if note else 'NULL'},
                                    ENTRANCE_NOTE = {f"'{entrance_note_escaped}'" if entrance_note else 'NULL'},
                                    OUTDOOR_UNIT_MODEL = '{outdoor_unit_model.replace("'", "''")}',
                                    OUTDOOR_UNIT_SERIAL_NUMBER = '{outdoor_unit_serial.replace("'", "''")}',
                                    INDOOR_UNIT_MODEL = '{indoor_unit_model.replace("'", "''")}',
                                    INDOOR_UNIT_SERIAL_NUMBER = '{indoor_unit_serial.replace("'", "''")}',
                                    THERMOSTAT_TYPE = '{thermostat_type.replace("'", "''")}',
                                    UNIT_LOCATION = '{unit_location}',
                                    ACCESSIBILITY_LEVEL = '{accessibility_level}'
                                WHERE CUSTOMERID = '{edit_customer_id}'
                            """
                            
                            session.sql(update_query).collect()
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
        
        
        
############################################################
#######################################################################
def appointments():
    st.subheader("📅 Appointment Scheduling")
    session = get_session()

    # --- Step 1: Customer Selection ---
    st.subheader("1. Select Customer")
    search_query = st.text_input("Search by Name, Phone, Email, or Address", key="customer_search")
    
    # Fetch customers with error handling
    try:
        customers = session.sql(f"""
            SELECT customerid, name, phone FROM customers 
            WHERE NAME ILIKE '%{search_query}%' 
               OR PHONE ILIKE '%{search_query}%'
               OR EMAIL ILIKE '%{search_query}%'
               OR ADDRESS ILIKE '%{search_query}%'
            ORDER BY name
        """).collect()
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return

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
        ["Repair", "Maintenance", "Install", "Estimate"],
        index=0
    )

    # Get qualified technicians with error handling
    
    try:
        expertise_map = {"Repair": "EX1", 
            "Maintenance": "EX2", 
            "Install": "EX3", 
            "Estimate": "EX4"}
        technicians = session.sql(f"""
            SELECT e.employeeid, e.ename 
            FROM employees e
            JOIN employee_expertise ee ON e.employeeid = ee.employeeid
            WHERE ee.expertiseid = '{expertise_map[request_type]}'
        """).collect()
    except Exception as e:
        st.error(f"Error loading technicians: {str(e)}")
        return

    if not technicians:
        st.error("No qualified technicians available for this service type")
        return

    # --- Different Logic for Install vs Other Services ---
    if request_type == "Install":
        # Full-day booking for installations
        st.subheader("3. Select Installation Date")
        
        # Show 4 weeks of available dates
        start_date = datetime.now().date()
        dates = [start_date + timedelta(days=i) for i in range(28)]
        
        # Get booked installation days
        try:
            booked_days = session.sql(f"""
                SELECT DISTINCT DATE(scheduled_time) as day 
                FROM appointments 
                WHERE service_type = 'Install'
                AND DATE(scheduled_time) BETWEEN '{start_date}' AND '{start_date + timedelta(days=28)}'
            """).collect()
            booked_days = [row['DAY'] for row in booked_days]
        except Exception as e:
            st.error(f"Error checking availability: {str(e)}")
            return

        # Display calendar grid
        st.write("### Available Dates")
        cols = st.columns(7)
        for i, date in enumerate(dates):
            with cols[i % 7]:
                if date in booked_days:
                    st.button(
                        f"{date.strftime('%m/%d')}",
                        disabled=True,
                        key=f"install_day_{date}"
                    )
                else:
                    if st.button(
                        f"{date.strftime('%m/%d')}",
                        key=f"install_day_{date}"
                    ):
                        st.session_state.selected_install_date = date
        
        # Handle installation booking
        if 'selected_install_date' in st.session_state:
            date = st.session_state.selected_install_date
            st.success(f"Selected installation date: {date.strftime('%A, %B %d')}")
            
            # Technician selection
            primary_tech = st.selectbox(
                "Primary Technician",
                options=[t['EMPLOYEEID'] for t in technicians],
                format_func=lambda x: next(t['ENAME'] for t in technicians if t['EMPLOYEEID'] == x),
                key="primary_install_tech"
            )
            
            secondary_techs = [t for t in technicians if t['EMPLOYEEID'] != primary_tech]
            secondary_tech = st.selectbox(
                "Additional Technician (Optional)",
                options=[""] + [t['EMPLOYEEID'] for t in secondary_techs],
                format_func=lambda x: next(t['ENAME'] for t in technicians if t['EMPLOYEEID'] == x) if x else "None",
                key="secondary_install_tech"
            )
            
            notes = st.text_area("Installation Notes")
            
            if st.button("Book Installation"):
                try:
                    # Check availability again
                    existing = session.sql(f"""
                        SELECT 1 FROM appointments
                        WHERE technicianid = '{primary_tech}'
                        AND DATE(scheduled_time) = '{date}'
                        LIMIT 1
                    """).collect()
                    
                    if existing:
                        st.error("Technician no longer available")
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
                            '{datetime.combine(date, time(8,0))}',
                            'Install',
                            '{notes.replace("'", "''")}',
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
                                '{notes.replace("'", "''")}',
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
        
        # Create calendar with 2-hour slots (8AM-6PM)
        time_slots = [time(hour) for hour in range(8, 19, 2)]
        
        # Get existing appointments
        try:
            appointments = session.sql(f"""
                SELECT * FROM appointments
                WHERE DATE(scheduled_time) BETWEEN '{start_date}' AND '{start_date + timedelta(days=6)}'
                AND sta_tus != 'cancelled'
            """).collect()
        except Exception as e:
            st.error(f"Error loading appointments: {str(e)}")
            return
        
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
                            
                            try:
                                # Check schedule availability
                                schedule_available = session.sql(f"""
                                    SELECT 1 FROM employee_schedules
                                    WHERE employeeid = '{tech_id}'
                                    AND schedule_date = '{day}'
                                    AND start_time <= '{time_slot}'
                                    AND end_time >= '{(time_slot.hour + 2) % 24}:{time_slot.minute}'
                                    LIMIT 1
                                """).collect()
                                
                                # Check appointment conflicts
                                appointment_conflict = session.sql(f"""
                                    SELECT 1 FROM appointments
                                    WHERE technicianid = '{tech_id}'
                                    AND scheduled_time >= '{slot_start}'
                                    AND scheduled_time < '{slot_end}'
                                    AND sta_tus != 'cancelled'
                                    LIMIT 1
                                """).collect()
                                
                                if schedule_available and not appointment_conflict:
                                    available_techs.append(tech)
                            except Exception as e:
                                st.error(f"Error checking availability: {str(e)}")
                                continue
                        
                        slot_label = f"{time_slot.hour}-{(time_slot.hour+2)%12 or 12}"
                        
                        if available_techs:
                            if st.button(
                                slot_label,
                                key=f"slot_{day}_{time_slot}",
                                help=f"Available: {len(available_techs)} techs",
                                on_click=lambda d=day, ts=time_slot: st.session_state.update({
                                    'selected_slot': {
                                        'datetime': datetime.combine(d, ts),
                                        'techs': available_techs
                                    }
                                })
                            ):
                                pass
                        else:
                            st.button(
                                slot_label,
                                disabled=True,
                                key=f"slot_{day}_{time_slot}_disabled"
                            )
        
        # Handle slot selection
        if 'selected_slot' in st.session_state:
            slot = st.session_state.selected_slot
            time_range = f"{slot['datetime'].hour}-{slot['datetime'].hour+2}"
            st.success(f"Selected: {slot['datetime'].strftime('%A %m/%d')} {time_range}")
            
            # Technician selection
            primary_tech = st.selectbox(
                "Primary Technician",
                options=[t['EMPLOYEEID'] for t in slot['techs']],
                format_func=lambda x: next(t['ENAME'] for t in slot['techs'] if t['EMPLOYEEID'] == x),
                key=f"primary_{slot['datetime'].timestamp()}"
            )
            
            secondary_techs = [t for t in slot['techs'] if t['EMPLOYEEID'] != primary_tech]
            secondary_tech = st.selectbox(
                "Additional Technician (Optional)",
                options=[""] + [t['EMPLOYEEID'] for t in secondary_techs],
                format_func=lambda x: next(t['ENAME'] for t in slot['techs'] if t['EMPLOYEEID'] == x) if x else "None",
                key=f"secondary_{slot['datetime'].timestamp()}"
            )
            
            notes = st.text_area("Service Notes")
            
            if st.button("Book Appointment"):
                try:
                    # Final availability check
                    existing = session.sql(f"""
                        SELECT 1 FROM appointments
                        WHERE technicianid = '{primary_tech}'
                        AND scheduled_time >= '{slot['datetime']}'
                        AND scheduled_time < '{slot['datetime'] + timedelta(hours=2)}'
                        LIMIT 1
                    """).collect()
                    
                    if existing:
                        st.error("Time slot no longer available")
                        st.rerun()
                    
                    # Create appointment
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
                            '{notes.replace("'", "''")}',
                            'scheduled'
                        )
                    """).collect()
                    
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
                                '{notes.replace("'", "''")}',
                                'scheduled'
                            )
                        """).collect()
                    
                    st.success("Appointment booked successfully!")
                    del st.session_state.selected_slot
                    st.rerun()
                
                except Exception as e:
                    st.error(f"Error creating appointment: {str(e)}")

    # --- Current Appointments Display ---
    st.subheader("📅 Current Appointments This Week")
    try:
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
                use_container_width=True,
                column_config={
                    "Date": "Date",
                    "Time": "Time Slot",
                    "Customer": "Customer",
                    "Technician": "Technician",
                    "Service": "Service Type",
                    "Status": "Status"
                }
            )
        else:
            st.info("No appointments scheduled for this week")
            
    except Exception as e:
        st.error(f"Error loading appointments: {str(e)}")

    # Navigation controls
    if st.button("🔄 Refresh Appointments"):
        st.rerun()

#######################################################################
#######################################################################

##################################   
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
import streamlit as st
def main_app():
    st.sidebar.title(f"Welcome {st.session_state.user_name}")

    # Available tabs based on roles
    available_tabs = set()
    for role in st.session_state.roles:
        available_tabs.update(ROLE_ACCESS.get(role.lower(), []))

    # Add the "profile" tab for all employees
    available_tabs.add("profile")

    # Define tab order
    tab_order = ['Home', 'profile', 'customers', 'appointments', 
               'admin_tables']
    
    available_tabs = [tab for tab in tab_order if tab in available_tabs]

    # Sidebar navigation
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
