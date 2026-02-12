"""Outlook email resource for Dagster notifications using SMTP."""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional, Union

from dagster import ConfigurableResource, get_dagster_logger
from decouple import config

logger = get_dagster_logger()


class OutlookEmailResource(ConfigurableResource):
    """Outlook email resource for sending notifications via SMTP.

    This resource handles sending email notifications through Outlook's SMTP server.
    It supports STARTTLS authentication and can send to multiple recipients.
    """

    email_from: str
    email_password: str
    email_to: List[str]
    smtp_host: str = "smtp-mail.outlook.com"
    smtp_port: int = 587
    smtp_type: str = "STARTTLS"  # STARTTLS or SSL
    smtp_user: Optional[str] = None  # If None, uses email_from

    def send_email(
        self,
        subject: str,
        body: str,
        to_emails: Optional[List[str]] = None,
        html_body: Optional[str] = None,
    ) -> None:
        """Send an email via Outlook SMTP.

        Args:
            subject: Email subject line
            body: Plain text email body
            to_emails: List of recipient email addresses. If None, uses self.email_to
            html_body: Optional HTML email body. If provided, sends multipart email.

        Raises:
            smtplib.SMTPException: If email sending fails
        """
        recipients = to_emails or self.email_to
        smtp_user = self.smtp_user or self.email_from

        # Create message
        if html_body:
            msg: Union[MIMEMultipart, MIMEText] = MIMEMultipart("alternative")
            msg.attach(MIMEText(body, "plain"))
            msg.attach(MIMEText(html_body, "html"))
        else:
            msg = MIMEText(body, "plain")

        msg["Subject"] = subject
        msg["From"] = self.email_from
        msg["To"] = ", ".join(recipients)

        try:
            # Connect to SMTP server
            if self.smtp_type.upper() == "SSL":
                server: Union[smtplib.SMTP, smtplib.SMTP_SSL] = smtplib.SMTP_SSL(
                    self.smtp_host, self.smtp_port
                )
            else:  # STARTTLS (default for Outlook)
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)
                server.starttls()

            # Login and send
            server.login(smtp_user, self.email_password)
            server.send_message(msg, from_addr=self.email_from, to_addrs=recipients)
            server.quit()

            logger.info(
                f"Email sent successfully to {len(recipients)} recipient(s)",
                extra={"subject": subject, "recipients": recipients},
            )

        except smtplib.SMTPException as e:
            logger.error(
                f"Failed to send email: {e}",
                extra={"subject": subject, "recipients": recipients},
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error sending email: {e}",
                extra={"subject": subject, "recipients": recipients},
                exc_info=True,
            )
            raise

    @classmethod
    def from_config(cls) -> "OutlookEmailResource":
        """Create resource from environment variables.

        Returns:
            Configured OutlookEmailResource instance

        Environment Variables:
            OUTLOOK_EMAIL_FROM: Sender email address
            OUTLOOK_EMAIL_PASSWORD: Email password or app password
            OUTLOOK_EMAIL_TO: Comma-separated list of recipient emails
            OUTLOOK_SMTP_HOST: SMTP host (default: smtp-mail.outlook.com)
            OUTLOOK_SMTP_PORT: SMTP port (default: 587)
            OUTLOOK_SMTP_TYPE: SMTP type - STARTTLS or SSL (default: STARTTLS)
            OUTLOOK_SMTP_USER: SMTP username (default: same as email_from)
        """
        email_to_str = config("OUTLOOK_EMAIL_TO", default="")
        email_to = [email.strip() for email in email_to_str.split(",") if email.strip()]

        return cls(
            email_from=config("OUTLOOK_EMAIL_FROM", default=""),
            email_password=config("OUTLOOK_EMAIL_PASSWORD", default=""),
            email_to=email_to,
            smtp_host=config("OUTLOOK_SMTP_HOST", default="smtp-mail.outlook.com"),
            smtp_port=config("OUTLOOK_SMTP_PORT", default=587, cast=int),
            smtp_type=config("OUTLOOK_SMTP_TYPE", default="STARTTLS"),
            smtp_user=config("OUTLOOK_SMTP_USER", default=None),
        )
