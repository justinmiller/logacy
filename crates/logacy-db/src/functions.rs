use diesel::sql_types::Text;

diesel::define_sql_function!(fn strftime(format: Text, date: Text) -> Text);
