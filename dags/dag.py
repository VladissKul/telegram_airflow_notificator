from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.notifications.basenotifier import BaseNotifier
from telegram import Bot
import asyncio
import os


class TelegramNotification(BaseNotifier):

    @staticmethod
    async def send_tg_notifier(notifier_message):

        telegram_bot_token = ''  # токен бота
        telegram_chat_id = ''  # id чата

        bot = Bot(token=telegram_bot_token)
        await bot.sendMessage(chat_id=telegram_chat_id, text=notifier_message, parse_mode="Markdown")

    def notify(self, context):

        task_id = context["ti"].task_id
        task_state = context["ti"].state
        task_log_url = context["ti"].log_url
        dag_name = context["ti"].dag_id

        message_template = (f"Dag name: `{dag_name}` \n"
                            f"Task id: `{task_id}` \n"
                            f"Task State: `🔴{task_state}🔴` \n"
                            f"Task Log URL: `{task_log_url}` \n"
                            )

        asyncio.run(self.send_tg_notifier(notifier_message=message_template))

args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 20),
}

dag = DAG(
    dag_id='telegram_notification_test',
    default_args=args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    on_failure_callback=TelegramNotification(),
)

start = DummyOperator(task_id="start_task", dag=dag)


third_task = BashOperator(
    task_id='third_task',
    bash_command="""ffff""",
    dag=dag)

end = DummyOperator(task_id="end_task", dag=dag)

start >> third_task >> end
