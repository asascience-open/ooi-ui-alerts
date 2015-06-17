# ooi-ui-alerts
Handles the Alerts coming from the uFrame QPID Service for the OOI project.

- Workflow
 - Retrieves the messages from the uFrame QPID JMS server
 - Populates the OOI-UI PostgreSQL database
 - Sends notification message(s) to ooi-ui-services to trigger UX updates, email and SMS notifications as defined by the alert and user settings
 - ooi-ui utilizes HTML5 Server-Sent Events (SSEs) for live updates based on new ooi-ui-services routes

- Operations
 - Contained in a separate Docker container
 - Runs periodically via cron job and/or supervisord
