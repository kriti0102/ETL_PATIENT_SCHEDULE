import pandas as pd
import re
import logging
from sqlalchemy import create_engine

MILESTONE_MAPPING = {
    "pre-op": "operation",
    "post-op": "operation",
    "po": "operation"
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# REGEX function for offset days and schedule_milestone_slug
def parse_schedule_slug(schedule_slug):
    def convert_to_days(value, unit, is_post):
        # Converts units to days with pre/post offset handling
        if unit == 'd':
            return value if is_post else -value
        elif unit == 'w':
            return value * 7 if is_post else -value * 7
        elif unit == 'm':
            return value * 30 if is_post else -value * 30
        elif unit == 'y':
            return value * 365 if is_post else -value * 365
        else:
            raise ValueError(f"Unsupported unit: {unit}")

    try:
        if not isinstance(schedule_slug, str) or schedule_slug.strip() == "":
            # Return default values for non-string or empty slugs
            return None, None, ""

        match = re.match(r"^(\d+)([dwmqy])-(\d+)([dwmqy])-(.+)$", schedule_slug)
        if match:
            start_value, start_unit = int(match.group(1)), match.group(2)
            end_value, end_unit = int(match.group(3)), match.group(4)
            milestone = match.group(5)

            is_post = "post" in milestone.lower()
            start_offset = convert_to_days(start_value, start_unit, is_post)
            end_offset = convert_to_days(end_value, end_unit, is_post)
            return start_offset, end_offset, MILESTONE_MAPPING.get(milestone, milestone)
        
        match = re.match(r"^(\d+)([dwmqy])-(pre)-(\d+)([dwmqy])(po)$", schedule_slug)
        
        if match:
            start_value, start_unit = int(match.group(1)), match.group(2)
            end_value, end_unit = int(match.group(4)), match.group(5)
            start_offset =  convert_to_days(start_value ,start_unit , False)
            end_offset = convert_to_days(end_value, end_unit,True)
            milestone = "po"
            return start_offset, end_offset, MILESTONE_MAPPING.get(milestone, milestone)

        match = re.match(r"^(\d+)([dwmqy])-(.+)", schedule_slug)
        if match:
            value, unit = int(match.group(1)), match.group(2)
            milestone = match.group(3)

            is_post = "post" in milestone.lower()
            if is_post:
                end_offset = convert_to_days(value, unit, is_post)
                return 0, end_offset, MILESTONE_MAPPING.get(milestone, milestone)
            else:
                start_offset = convert_to_days(value, unit, is_post)
                return start_offset, 0, MILESTONE_MAPPING.get(milestone, milestone)

        match = re.match(r"(.+)", schedule_slug)
        if match:
            milestone = match.group(1)
            return None, None, MILESTONE_MAPPING.get(milestone, milestone)

    except Exception as e:
        logging.error(f"Error parsing schedule_slug '{schedule_slug}': {e}")
    return None, None, ""

def build_patient_journey_schedule_window(input_db_url, output_table_name):
    try:
        engine = create_engine(input_db_url)
        
        with engine.connect() as connection:
            tables = {name: pd.read_sql_query(f"SELECT * FROM {name}", con=connection)
                      for name in ["patient", "patient_journey", "journey_activity", "activity", "schedule"]}

            patient = tables['patient'][['id']].rename(columns={'id': 'patient_id'})
            patient_journey = tables['patient_journey'][['patient_id', 'journey_id', 'invitation_date',
                                                        'registration_date', 'operation_date', 'discharge_date',
                                                        'consent_date']]
            journey_activity = tables['journey_activity'][['journey_id', 'activity_id']]
            activity = tables['activity'][['id', 'content_slug', 'schedule_id']].rename(columns={'id': 'activity_id'})
            schedule = tables['schedule'][['id', 'slug']].rename(columns={'id': 'schedule_id'})

            schedule['slug'] = schedule['slug'].fillna('').astype(str)

            joined_data = (patient
                           .merge(patient_journey, on="patient_id", how="left")
                           .merge(journey_activity, on="journey_id", how="left")
                           .merge(activity, on="activity_id", how="left")
                           .merge(schedule, on="schedule_id", how="left"))
            
            joined_data = joined_data.drop_duplicates(subset=['patient_id', 'journey_id', 'activity_id'])

            joined_data[["schedule_start_offset_days", "schedule_end_offset_days", "schedule_milestone_slug"]] = joined_data["slug"].apply(
                lambda slug: pd.Series(parse_schedule_slug(slug))
            )

            final_columns = {
                "patient_id": "patient_id",
                "journey_id": "patient_journey_id",
                "activity_id": "activity_id",
                "content_slug": "activity_content_slug",
                "schedule_id": "schedule_id",
                "slug": "schedule_slug",
                "schedule_start_offset_days": "schedule_start_offset_days",
                "schedule_end_offset_days": "schedule_end_offset_days",
                "schedule_milestone_slug": "schedule_milestone_slug",
            }
            final_data = joined_data[list(final_columns.keys())].rename(columns=final_columns)

            final_data.to_sql(output_table_name, con=engine, if_exists="replace", index=False)
            logging.info(f"Data pipeline complete! Output written to table: {output_table_name}")

    except Exception as e:
        logging.error(f"An error occurred while building the patient journey schedule window: {e}")


if __name__ == "__main__":

    input_db_url = "postgresql://localhost:5432/msk_db" # Replace as per local setup
    output_table_name = "patient_journey_schedule_window"

    build_patient_journey_schedule_window(input_db_url, output_table_name)
