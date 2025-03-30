import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pyxirr
import calendar
import numpy as np # Needed for CAGR calculation if using numpy's power function

# --- Configuration ---
EXCEL_FILE_PATH = 'Mutual-Funds-India-Historical-NAV-Report.xlsx'


DATE_COLUMN_NAME = 'NAV Date'
NAV_COLUMN_NAME = 'NAV (Rs)'

DATE_FORMAT = '%d-%m-%Y' # Example: 28-02-2025

SIP_AMOUNT = 10000.0
START_YEAR = 2022
START_MONTH = 3
END_YEAR = 2025 # Adjusted back as per user's last provided code
END_MONTH = 2
SIP_DAY = 1 # Day of the month the SIP investment occurs

# --- Future Value Projection Configuration ---
PROJECT_FUTURE_YEARS = 5 

# --- Helper Functions ---

def get_last_day_of_month(year, month):
    """Gets the last day of the given month and year."""
    return calendar.monthrange(year, month)[1]

def generate_sip_dates(start_year, start_month, end_year, end_month, day):
    """Generates a list of SIP dates, handling month ends correctly."""
    dates = []
    start_day = min(day, get_last_day_of_month(start_year, start_month))
    current_date = datetime(start_year, start_month, start_day)
    end_target_month_last_day = get_last_day_of_month(end_year, end_month)
    end_target_date = datetime(end_year, end_month, min(day, end_target_month_last_day))

    while current_date <= end_target_date:
        dates.append(current_date)
        current_date += relativedelta(months=1)
        last_day_next_month = get_last_day_of_month(current_date.year, current_date.month)
        actual_day = min(day, last_day_next_month)
        current_date = current_date.replace(day=actual_day)
    return dates

# --- Main Calculation Logic ---

try:
    # 1. Load and Prepare NAV Data from Excel
    print(f"Loading data from Excel file: {EXCEL_FILE_PATH}...")
    # If SHEET_NAME is None, pandas reads the first sheet by default
    nav_data = pd.read_excel(EXCEL_FILE_PATH)
    print("Data loaded successfully.")

    nav_data.columns = nav_data.columns.str.strip()
    print(f"Available columns: {nav_data.columns.tolist()}")

    if DATE_COLUMN_NAME not in nav_data.columns:
        raise KeyError(f"Date column '{DATE_COLUMN_NAME}' not found.")
    if NAV_COLUMN_NAME not in nav_data.columns:
        raise KeyError(f"NAV column '{NAV_COLUMN_NAME}' not found.")

    print(f"Converting '{DATE_COLUMN_NAME}' to dates using format '{DATE_FORMAT}'...")
    try:
        nav_data['Date'] = pd.to_datetime(nav_data[DATE_COLUMN_NAME], format=DATE_FORMAT, errors='coerce')
    except Exception as e:
        print(f"\nError during date conversion: {e}")
        print(f"Check DATE_FORMAT ('{DATE_FORMAT}') vs Excel column '{DATE_COLUMN_NAME}'.")
        print("Example dates in column:", nav_data[DATE_COLUMN_NAME].head())
        exit()

    invalid_dates = nav_data['Date'].isna().sum()
    if invalid_dates > 0:
        print(f"Warning: {invalid_dates} rows had issues converting '{DATE_COLUMN_NAME}' to a date. These rows ignored.")
        nav_data.dropna(subset=['Date'], inplace=True)

    if nav_data.empty:
        print(f"Error: No valid date entries found after conversion with format '{DATE_FORMAT}'.")
        exit()

    print(f"Converting '{NAV_COLUMN_NAME}' to numeric...")
    nav_data['NAV'] = pd.to_numeric(nav_data[NAV_COLUMN_NAME], errors='coerce')
    invalid_navs = nav_data['NAV'].isna().sum()
    if invalid_navs > 0:
         print(f"Warning: {invalid_navs} rows had non-numeric values in '{NAV_COLUMN_NAME}' and were ignored.")
         nav_data.dropna(subset=['NAV'], inplace=True)

    if nav_data.empty:
        print(f"Error: No valid numeric NAV entries found.")
        exit()

    nav_data.set_index('Date', inplace=True)
    nav_data.sort_index(inplace=True)

    filter_start = datetime(START_YEAR, START_MONTH, 1) - relativedelta(days=7)
    end_month_last_day = get_last_day_of_month(END_YEAR, END_MONTH)
    filter_end = datetime(END_YEAR, END_MONTH, end_month_last_day) + relativedelta(days=7)
    nav_data = nav_data[filter_start:filter_end]

    if nav_data.empty:
        print(f"Error: No NAV data found between {filter_start.strftime('%Y-%m-%d')} and {filter_end.strftime('%Y-%m-%d')}.")
        exit()

    print(f"NAV data prepared. Found {len(nav_data)} records from {nav_data.index.min().strftime('%Y-%m-%d')} to {nav_data.index.max().strftime('%Y-%m-%d')}.")

    # 2. Generate SIP Dates
    sip_dates = generate_sip_dates(START_YEAR, START_MONTH, END_YEAR, END_MONTH, SIP_DAY)
    if not sip_dates:
        print("Error: No SIP dates generated.")
        exit()
    print(f"Generated {len(sip_dates)} SIP dates from {sip_dates[0].strftime('%d-%b-%Y')} to {sip_dates[-1].strftime('%d-%b-%Y')}.")

    # 3. Process Investments
    total_units = 0.0
    total_investment = 0.0
    transactions = [] # For XIRR
    processed_investments = 0
    monthly_performance_data = [] # For tracking monthly value and return
    last_value_after_sip = 0.0     # Track value after previous SIP
    first_investment_date = None   # Track the date of the very first investment

    print("Processing Investments...")
    for sip_date in sip_dates:
        actual_investment_date = None
        nav_on_sip_date = None
        try:
            # Find NAV on or after sip_date
            future_navs = nav_data.loc[sip_date:]
            if not future_navs.empty:
                nav_on_sip_date = future_navs.iloc[0]['NAV']
                actual_investment_date = future_navs.index[0]
            else:
                print(f"Warning: Could not find NAV for SIP date {sip_date.strftime('%Y-%m-%d')} or later. Skipping.")
                continue
        except KeyError:
             # Handle cases where sip_date might be before the first date in the filtered nav_data index
             available_dates = nav_data.index[nav_data.index >= sip_date]
             if not available_dates.empty:
                 actual_investment_date = available_dates[0]
                 nav_on_sip_date = nav_data.loc[actual_investment_date, 'NAV']
                 print(f"Note: Using next available NAV date {actual_investment_date.strftime('%Y-%m-%d')} for SIP date {sip_date.strftime('%Y-%m-%d')}.")
             else:
                 print(f"Warning: SIP date {sip_date.strftime('%Y-%m-%d')} before first available NAV date ({nav_data.index.min().strftime('%Y-%m-%d')}). Skipping.")
                 continue
        except Exception as e:
             print(f"Error looking up NAV for {sip_date.strftime('%Y-%m-%d')}: {e}. Skipping.")
             continue

        if nav_on_sip_date is None or pd.isna(nav_on_sip_date) or nav_on_sip_date <= 0:
             print(f"Warning: Invalid NAV ({nav_on_sip_date}) found near {sip_date.strftime('%Y-%m-%d')}. Skipping.")
             continue

        # --- Calculations for this SIP ---
        units_bought = SIP_AMOUNT / nav_on_sip_date
        value_before_investment = total_units * nav_on_sip_date # Value of existing units at current NAV

        # Update totals
        total_units += units_bought
        total_investment += SIP_AMOUNT
        processed_investments += 1
        value_after_investment = total_units * nav_on_sip_date # Value incl. current SIP

        # Track the first investment date
        if first_investment_date is None:
            first_investment_date = actual_investment_date

        # Add transaction for XIRR
        transactions.append((actual_investment_date, -SIP_AMOUNT))

        # --- Calculate performance since last SIP ---
        period_return_pct = 0.0
        if processed_investments > 1 and last_value_after_sip > 0:
            period_gain_loss = value_before_investment - last_value_after_sip
            period_return_pct = (period_gain_loss / last_value_after_sip) * 100

        # Store monthly data
        monthly_performance_data.append({
            'Investment Date': actual_investment_date,
            'NAV': nav_on_sip_date,
            'Units Bought': units_bought,
            'Total Units': total_units,
            'Value After SIP': value_after_investment,
            'Period Return (%)': period_return_pct # Return since last SIP
        })

        # Update value for next iteration's comparison
        last_value_after_sip = value_after_investment

    if processed_investments == 0:
        print("\nError: No investments could be processed.")
        exit()

    # Create DataFrame for monthly performance
    monthly_df = pd.DataFrame(monthly_performance_data)

    # 4. Calculate Final Value
    end_period_day = get_last_day_of_month(END_YEAR, END_MONTH)
    end_period_date = datetime(END_YEAR, END_MONTH, end_period_day)
    # Find the last available NAV on or before the target end date
    final_nav_data = nav_data.loc[:end_period_date]

    if final_nav_data.empty:
         if not nav_data.empty:
              final_nav_data = nav_data.iloc[-1:] # Use the very last record
              final_nav_date = final_nav_data.index[0]
              print(f"Warning: No NAV data found on or before {end_period_date.strftime('%Y-%m-%d')}. Using the last available NAV record from {final_nav_date.strftime('%Y-%m-%d')}.")
         else:
              print("Error: Could not determine final NAV.")
              exit()
    else:
        # Get the last record from the filtered data up to the end date
        final_nav_data = final_nav_data.iloc[-1:]
        final_nav_date = final_nav_data.index[0]


    final_nav = final_nav_data['NAV'].iloc[0]
    final_value = total_units * final_nav

    # Add final redemption value for XIRR
    transactions.append((final_nav_date, final_value))

    # 5. Calculate Returns
    absolute_return = ((final_value - total_investment) / total_investment) * 100 if total_investment > 0 else 0

    dates = [t[0] for t in transactions]
    amounts = [t[1] for t in transactions]
    annualized_return_xirr = None
    cagr = None
    investment_duration_years = None

    # Calculate XIRR
    if len(dates) > 1 and len(amounts) > 1:
        try:
            dates_for_xirr = [pd.to_datetime(d) for d in dates]
            if not all(dates_for_xirr[i] <= dates_for_xirr[i+1] for i in range(len(dates_for_xirr)-1)):
                 print("\nWarning: Dates for XIRR are not monotonically increasing. Sorting them.")
                 sorted_transactions = sorted(zip(dates_for_xirr, amounts))
                 dates_for_xirr = [t[0] for t in sorted_transactions]
                 amounts = [t[1] for t in sorted_transactions]

            annualized_return_xirr = pyxirr.xirr(dates_for_xirr, amounts) * 100
        except ValueError as e:
            print(f"\nError calculating XIRR: {e}")
            print("Dates:", [d.strftime('%Y-%m-%d') for d in dates_for_xirr])
            print("Amounts:", [f"{a:,.2f}" for a in amounts])
        except Exception as e:
            print(f"\nCould not calculate XIRR due to an unexpected error: {e}")
    else:
        print("\nWarning: Not enough transactions for XIRR calculation.")

    # Calculate Simplified CAGR (Requires first investment date and final NAV date)
    if first_investment_date is not None and final_nav_date is not None and total_investment > 0 and final_value > 0:
        time_difference = final_nav_date - first_investment_date
        investment_duration_years = time_difference.days / 365.25

        if investment_duration_years > 0:
            try:
                # CAGR = ((Ending Value / Beginning Value) ** (1 / Number of Years)) - 1
                # Note: Using total_investment as 'Beginning Value' is a simplification for SIPs.
                cagr = (((final_value / total_investment) ** (1 / investment_duration_years)) - 1) * 100
            except Exception as e:
                print(f"\nCould not calculate CAGR: {e}") # Handle potential math errors (e.g., negative values if data error)
        elif investment_duration_years == 0:
             print("\nNote: CAGR cannot be calculated as investment duration is zero days.")
        else:
             print("\nNote: CAGR cannot be calculated due to negative investment duration (check dates).")

    # --- 7. Print Results ---
    print("\n--- SIP Calculation Results ---")
    print(f"Investment Period: {START_MONTH:02d}-{START_YEAR} to {END_MONTH:02d}-{END_YEAR} (SIP Day: {SIP_DAY})")
    if first_investment_date:
        print(f"First Investment Date Found: {first_investment_date.strftime('%d-%b-%Y')}")
    print(f"Monthly SIP Amount: Rs. {SIP_AMOUNT:,.2f}")
    print(f"Total Amount Invested: Rs. {total_investment:,.2f}")
    print(f"Number of Investments Processed: {processed_investments} / {len(sip_dates)}")
    print("-" * 30)
    print(f"Final NAV Date Used: {final_nav_date.strftime('%d-%b-%Y')}")
    print(f"Final NAV: Rs. {final_nav:.4f}")
    print(f"Total Units Accumulated: {total_units:.4f}")
    print(f"Final Investment Value: Rs. {final_value:,.2f}")
    print("-" * 30)
    print(f"Total Gain/Loss: Rs. {(final_value - total_investment):,.2f}")
    print(f"Absolute Return: {absolute_return:.2f}%")
    if annualized_return_xirr is not None:
        print(f"Annualized Return (XIRR): {annualized_return_xirr:.2f}%")
    else:
        print("Annualized Return (XIRR): Could not be calculated.")
    if cagr is not None:
         print(f"Simplified CAGR: {cagr:.2f}%")
    else:
         print("Simplified CAGR: Could not be calculated.")

    if cagr is not None and investment_duration_years is not None:
        print(f"      (Simplified CAGR calculated over approx. {investment_duration_years:.2f} years using total investment).")

    # --- Print Monthly Performance ---
    print("\n" + "-" * 60) # Wider separator
    print("--- Performance Between SIP Investments (Approx.) ---")
    print("-" * 60)
    if not monthly_df.empty:
        # Format the output DataFrame for better readability
        monthly_df_display = monthly_df.copy()
        monthly_df_display['Investment Date'] = monthly_df_display['Investment Date'].dt.strftime('%d-%b-%Y')
        monthly_df_display['NAV'] = monthly_df_display['NAV'].map('{:,.4f}'.format)
        monthly_df_display['Units Bought'] = monthly_df_display['Units Bought'].map('{:,.4f}'.format)
        monthly_df_display['Total Units'] = monthly_df_display['Total Units'].map('{:,.4f}'.format)
        monthly_df_display['Value After SIP'] = monthly_df_display['Value After SIP'].map('Rs. {:,.2f}'.format)
        # Handle potential NaN or infinite values in Period Return before formatting
        monthly_df_display['Period Return (%)'] = pd.to_numeric(monthly_df_display['Period Return (%)'], errors='coerce')
        monthly_df_display['Period Return (%)'] = monthly_df_display['Period Return (%)'].map(lambda x: '{:.2f}%'.format(x) if pd.notna(x) else 'N/A')

        # Use to_string for cleaner console output without index
        # Adjust display settings if needed for wide tables
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_columns', None)
        print(monthly_df_display.to_string(index=False, justify='right')) # Right-align columns
    else:
        print("No monthly performance data generated.")
    print("-" * 60)


except FileNotFoundError:
    print(f"Error: The file '{EXCEL_FILE_PATH}' was not found.")
except KeyError as e:
    print(f"\nError: Column {e} not found. Check configuration vs Excel columns.")
    print("Ensure exact spelling, case, and no extra spaces in column names.")
except ValueError as e:
    # Catch potential errors from date/numeric conversion if not handled earlier
    print(f"\nError processing data: {e}")
    print("Check date format or non-numeric data.")
except Exception as e:
    import traceback
    print(f"\nAn unexpected error occurred: {e}")
    print("\n--- Error Traceback ---")
    traceback.print_exc()
    print("-----------------------")
