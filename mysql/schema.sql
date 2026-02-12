CREATE TABLE IF NOT EXISTS application (
  id INT PRIMARY KEY,
  created_by TEXT,
  created_date TEXT,
  last_modified_by TEXT,
  last_modified_date TEXT,
  is_active TEXT,
  first_name TEXT,
  last_name TEXT,
  phone_number TEXT,
  city TEXT,
  postal_code TEXT,
  status TEXT,
  cv_url TEXT,
  is_review TEXT,
  is_match TEXT,
  is_contacted TEXT,
  is_rejected TEXT,
  score TEXT,
  email TEXT,
  job_id TEXT,
  user_id TEXT,
  conversation_id TEXT,
  country TEXT,
  is_notification_email TEXT,
  is_notification_sms TEXT,
  publisher_id TEXT
);

CREATE TABLE IF NOT EXISTS campaign (
  id INT PRIMARY KEY,
  created_by TEXT,
  created_date TEXT,
  last_modified_by TEXT,
  last_modified_date TEXT,
  is_active TEXT,
  campaign_name TEXT,
  budget_total TEXT,
  unit_budget TEXT,
  start_date TEXT,
  end_date TEXT,
  close_date TEXT,
  campaign_status TEXT,
  background_status TEXT,
  pacing_unit TEXT,
  company_id TEXT,
  number_of_pacing TEXT,
  recommend_budget TEXT,
  import_xml_url TEXT,
  bid_set TEXT,
  ats_type TEXT,
  ats_data TEXT
);

CREATE TABLE IF NOT EXISTS company (
  id INT PRIMARY KEY,
  created_by TEXT,
  created_date TEXT,
  last_modified_by TEXT,
  last_modified_date TEXT,
  is_active TEXT,
  name TEXT,
  is_agree_conditon TEXT,
  is_agree_sign_deal TEXT,
  sign_deal_user TEXT,
  billing_id TEXT,
  manage_type TEXT,
  customer_type TEXT,
  status TEXT,
  publisher_id TEXT,
  flat_rate TEXT,
  percentage_of_click TEXT,
  logo TEXT
);

CREATE TABLE IF NOT EXISTS conversation (
  id INT PRIMARY KEY,
  created_by TEXT,
  created_date TEXT,
  last_modified_by TEXT,
  last_modified_date TEXT,
  is_active TEXT,
  name TEXT,
  description TEXT
);

CREATE TABLE IF NOT EXISTS dev_user (
  id INT PRIMARY KEY,
  created_by TEXT,
  created_date TEXT,
  last_modified_by TEXT,
  last_modified_date TEXT,
  is_active TEXT,
  user_name TEXT,
  password TEXT,
  email TEXT,
  company_id TEXT,
  phone TEXT,
  user_type TEXT,
  status TEXT,
  login_type TEXT,
  profile_id TEXT,
  user_client_permission_id TEXT
);

CREATE TABLE IF NOT EXISTS events (
  id INT AUTO_INCREMENT PRIMARY KEY,
  job_id INT,
  dates TEXT,
  hours INT,
  disqualified_application INT,
  qualified_application INT,
  conversion INT,
  company_id INT,
  group_id INT,
  campaign_id INT,
  publisher_id INT,
  bid_set DOUBLE,
  clicks INT,
  spend_hour DOUBLE,
  sources TEXT,
  updated_at DATETIME
);

CREATE TABLE IF NOT EXISTS `group` (
  id INT PRIMARY KEY,
  created_by TEXT,
  created_date TEXT,
  last_modified_by TEXT,
  last_modified_date TEXT,
  is_active TEXT,
  name TEXT,
  budget_group TEXT,
  status TEXT,
  company_id TEXT,
  campaign_id TEXT,
  number_of_pacing TEXT
);

CREATE TABLE IF NOT EXISTS job (
  id INT PRIMARY KEY,
  created_by TEXT,
  created_date TEXT,
  last_modified_by TEXT,
  last_modified_date TEXT,
  is_active TEXT,
  title TEXT,
  description TEXT,
  work_schedule TEXT,
  radius_unit TEXT,
  location_state TEXT,
  location_list TEXT,
  role_location TEXT,
  resume_option TEXT,
  budget TEXT,
  status TEXT,
  error TEXT,
  template_question_id TEXT,
  template_question_name TEXT,
  question_form_description TEXT,
  redirect_url TEXT,
  start_date TEXT,
  end_date TEXT,
  close_date TEXT,
  group_id TEXT,
  minor_id TEXT,
  campaign_id TEXT,
  company_id TEXT,
  history_status TEXT,
  ref_id TEXT
);

CREATE TABLE IF NOT EXISTS job_location (
  id INT PRIMARY KEY,
  created_by TEXT,
  created_date TEXT,
  last_modified_by TEXT,
  last_modified_date TEXT,
  is_active TEXT,
  city_id TEXT,
  job_id TEXT,
  main_location TEXT,
  radius TEXT,
  reference_id TEXT,
  reference_url TEXT
);

CREATE TABLE IF NOT EXISTS master_publisher (
  id INT PRIMARY KEY,
  created_by TEXT,
  created_date TEXT,
  last_modified_by TEXT,
  last_modified_date TEXT,
  is_active TEXT,
  publisher_name TEXT,
  email TEXT,
  access_token TEXT,
  publisher_type TEXT,
  publisher_status TEXT,
  publisher_frequency TEXT,
  curency TEXT,
  time_zone TEXT,
  cpc_increment TEXT,
  bid_reading TEXT,
  min_bid TEXT,
  max_bid TEXT,
  countries TEXT,
  data_sharing TEXT
);

CREATE TABLE IF NOT EXISTS user_client_assignee (
  id INT PRIMARY KEY,
  created_by TEXT,
  created_date TEXT,
  last_modified_by TEXT,
  last_modified_date TEXT,
  is_active TEXT,
  user_id TEXT,
  company_id TEXT
);

CREATE TABLE IF NOT EXISTS user_role (
  id INT PRIMARY KEY,
  created_by TEXT,
  created_date TEXT,
  last_modified_by TEXT,
  last_modified_date TEXT,
  is_active TEXT,
  user_id TEXT,
  role_id TEXT,
  status TEXT
);
