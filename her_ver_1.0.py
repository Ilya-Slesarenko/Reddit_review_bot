import config as conf
import logging, random, warnings, base64
from aiogram import Bot, Dispatcher, executor, types
from hint_info import RecommendAdvice
from filters import IsAdminFilter

# log level
logging.basicConfig(level=logging.INFO)
warnings.filterwarnings('ignore')

# bot init
bot = Bot(token=conf.TOKEN)
dp = Dispatcher(bot)
dp.filters_factory.bind(IsAdminFilter)


# advice_1 feedback
@dp.message_handler(is_admin=True)
async def GiveRecommendation(message: types.Message):
    # если команда не ответ на сообщение, то крикнуть
    if not message.reply_to_message:
        split_text = message.text.split(" ")
        chat_id = message.chat.id
        feedback = RecommendAdvice(split_text).Summarize()
        try:
            if len(feedback) >1:
                await message.answer(feedback[0])
                await bot.send_photo(chat_id, photo=feedback[1])
                await bot.send_photo(chat_id, photo=feedback[2])
            else:
                await message.answer('Whoops, need to check the code')
        except TypeError:
            pass
        return


# run long-polling
if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)