"""
Data loading functions for product, member, and employee tables.
"""
import pandas as pd
from vertical_affinity.config import PRODUCT_MYSQL, EMPLOYEE_MYSQL, MEMBER_MYSQL


def load_product_table(engine):
    """
    Load product table from MySQL database.
    
    Args:
        engine: SQLAlchemy engine for MySQL connection
        
    Returns:
        pd.DataFrame: Product data
    """
    try:
        product = pd.read_sql_table(
            table_name=PRODUCT_MYSQL,
            con=engine
        )
        print(f"\n✅ 成功加载表 '{PRODUCT_MYSQL}'，共 {len(product)} 行。")
        return product
    except Exception as e:
        print(f"❌ 加载表时出错: {e}")
        raise


def load_employee_table(engine):
    """
    Load employee table from MySQL database.
    
    Args:
        engine: SQLAlchemy engine for MySQL connection
        
    Returns:
        pd.DataFrame: Employee data
    """
    try:
        employee = pd.read_sql_table(
            table_name=EMPLOYEE_MYSQL,
            con=engine
        )
        print(f"\n✅ 成功加载表 '{EMPLOYEE_MYSQL}'，共 {len(employee)} 行。")
        return employee
    except Exception as e:
        print(f"❌ 加载表时出错: {e}")
        raise


def load_member_table(engine):
    """
    Load member table from MySQL database.
    
    Args:
        engine: SQLAlchemy engine for MySQL connection
        
    Returns:
        pd.DataFrame: Member data
    """
    try:
        member = pd.read_sql_table(
            table_name=MEMBER_MYSQL,
            con=engine
        )
        print(f"\n✅ 成功加载表 '{MEMBER_MYSQL}'，共 {len(member)} 行。")
        return member
    except Exception as e:
        print(f"❌ 加载表时出错: {e}")
        raise


def filter_employees(member_df, employee_df):
    """
    Filter out employees from member dataframe.
    
    Args:
        member_df: Member dataframe
        employee_df: Employee dataframe
        
    Returns:
        pd.DataFrame: Filtered member data without employees
    """
    employee_ids = employee_df['member_uid'].unique()
    member_filtered = member_df[~member_df['member_uid'].isin(employee_ids)].copy()
    print(f"\n✅ 成功过滤员工，共 {len(member_filtered)} 行。")
    return member_filtered

