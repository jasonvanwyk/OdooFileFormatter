"""
Data transformation utilities for converting to Odoo format
"""

import pandas as pd
import numpy as np
from decimal import Decimal, ROUND_HALF_UP


class DataTransformer:
    """Handles transformation of source data to Odoo product template format"""

    def __init__(self):
        # Define the final Odoo column structure
        self.odoo_columns = [
            'Internal Reference',
            'Name',
            'Can be Sold',
            'Can be Purchased',
            'Product Type',
            'Category',
            'Sale Price',
            'Cost',
            'Quantity On Hand',
            'Weight',
            'Volume',
            'Barcode',
            'Description for Quotations',
            'Description for Receipts',
            'Customer Taxes',
            'Vendor Taxes'
        ]

    def transform(self, df, column_mapping):
        """
        Transform source dataframe to Odoo product template format

        Args:
            df (pandas.DataFrame): Source dataframe
            column_mapping (dict): Mapping of source columns to Odoo fields

        Returns:
            pandas.DataFrame: Transformed dataframe ready for Odoo import
        """
        # Create new dataframe with Odoo structure
        result_df = pd.DataFrame(columns=self.odoo_columns)

        # Initialize with default values
        num_rows = len(df)
        result_df = pd.DataFrame(index=range(num_rows), columns=self.odoo_columns)

        # Set default values for required fields
        result_df['Can be Sold'] = True
        result_df['Can be Purchased'] = True
        result_df['Product Type'] = 'Goods'
        result_df['Category'] = 'All'

        # Map the columns based on user configuration
        for source_col_idx, odoo_field in column_mapping.items():
            if source_col_idx < len(df.columns):
                source_data = df.iloc[:, source_col_idx]

                if odoo_field == 'internal_reference':
                    result_df['Internal Reference'] = self._clean_text_data(source_data)

                elif odoo_field == 'name':
                    result_df['Name'] = self._clean_text_data(source_data)

                elif odoo_field == 'list_price':
                    result_df['Sale Price'] = self._clean_float_data(source_data)

                elif odoo_field == 'standard_price':
                    result_df['Cost'] = self._clean_float_data(source_data)

                elif odoo_field == 'qty_available':
                    result_df['Quantity On Hand'] = self._clean_float_data(source_data)

        # Remove rows where required fields are empty
        result_df = result_df.dropna(subset=['Internal Reference', 'Name'])

        # Fill empty optional fields with appropriate defaults
        result_df['Sale Price'] = result_df['Sale Price'].fillna(0.0)
        result_df['Cost'] = result_df['Cost'].fillna(0.0)
        result_df['Quantity On Hand'] = result_df['Quantity On Hand'].fillna(0.0)

        # Fill other optional fields with empty strings
        for col in ['Weight', 'Volume', 'Barcode', 'Description for Quotations',
                   'Description for Receipts', 'Customer Taxes', 'Vendor Taxes']:
            result_df[col] = result_df[col].fillna('')

        return result_df

    def _clean_text_data(self, series):
        """
        Clean text data by removing extra whitespace and handling NaN values

        Args:
            series (pandas.Series): Source text data

        Returns:
            pandas.Series: Cleaned text data
        """
        return series.astype(str).str.strip().replace('nan', '')

    def _clean_float_data(self, series, decimal_places=2):
        """
        Clean numeric data and format to specified decimal places

        Args:
            series (pandas.Series): Source numeric data
            decimal_places (int): Number of decimal places

        Returns:
            pandas.Series: Cleaned numeric data
        """
        # Convert to numeric, coercing errors to NaN
        numeric_series = pd.to_numeric(series, errors='coerce')

        # Round to specified decimal places
        rounded_series = numeric_series.round(decimal_places)

        return rounded_series

    def _format_currency(self, value, decimal_places=2):
        """
        Format a single currency value to specified decimal places

        Args:
            value: Numeric value to format
            decimal_places (int): Number of decimal places

        Returns:
            float: Formatted value
        """
        if pd.isna(value):
            return 0.0

        try:
            # Use Decimal for precise rounding
            decimal_value = Decimal(str(value))
            rounded_value = decimal_value.quantize(
                Decimal('0.' + '0' * decimal_places),
                rounding=ROUND_HALF_UP
            )
            return float(rounded_value)
        except:
            return 0.0

    def validate_transformed_data(self, df):
        """
        Validate the transformed data for Odoo compatibility

        Args:
            df (pandas.DataFrame): Transformed dataframe

        Returns:
            dict: Validation results with errors and warnings
        """
        errors = []
        warnings = []

        # Check required fields
        if df['Internal Reference'].isna().any():
            errors.append("Some products are missing Internal Reference")

        if df['Name'].isna().any():
            errors.append("Some products are missing Name")

        # Check for duplicate internal references
        duplicates = df['Internal Reference'].duplicated()
        if duplicates.any():
            duplicate_refs = df[duplicates]['Internal Reference'].tolist()
            warnings.append(f"Duplicate Internal References found: {duplicate_refs}")

        # Check numeric fields
        numeric_fields = ['Sale Price', 'Cost', 'Quantity On Hand']
        for field in numeric_fields:
            if field in df.columns:
                if df[field].isna().any():
                    warnings.append(f"Some products have empty {field}")

                # Check for negative values
                if (df[field] < 0).any():
                    warnings.append(f"Some products have negative {field}")

        return {
            'errors': errors,
            'warnings': warnings,
            'valid': len(errors) == 0
        }