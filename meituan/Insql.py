import time
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# 使用utf8mb4编码连接MySql
engine = create_engine('mysql+pymysql://root:1234@localhost/spiderdb?charset=utf8mb4')
DBSession = sessionmaker(bind=engine)
SQLsession = DBSession()
Base = declarative_base()


# 商家数据表
class shop(Base):
    __tablename__ = 'meituan_shop'
    id = Column(Integer(), primary_key=True)
    shop_id = Column(String(100), comment='商家ID')
    shop_name = Column(String(300), comment='商家名称')
    shop_address = Column(String(500), comment='商家地址')
    shop_phone = Column(String(100), comment='商家电话')
    shop_openTime = Column(String(500), comment='营业时间')
    shop_avgScore = Column(String(100), comment='评分')
    shop_avgPrice = Column(String(100), comment='人均价格')
    shop_city = Column(String(100), comment='所在城市')
    log_date = Column(String(100), comment='记录日期')


# 顾客评论
class comment(Base):
    __tablename__ = 'meituan_comment'
    id = Column(Integer(), primary_key=True)
    shop_id = Column(String(100), comment='商家ID')
    reviewId = Column(String(100), comment='评论ID')
    userId = Column(String(100), comment='用户ID')
    userName = Column(String(100), comment='用户名')
    userScore = Column(String(100), comment='用户评分')
    comment = Column(String(3000), comment='评论内容')
    commentTime = Column(String(100), comment='评论时间')
    merchantComment = Column(String(3000), comment='商家回复')
    log_date = Column(String(100), comment='记录日期')


# 创建数据表
Base.metadata.create_all(engine)


# 写入商家信息
def shop_db(info_dict):
    temp_id = info_dict['shop_id']
    # 判断是否已存在记录
    info = SQLsession.query(shop).filter_by(shop_id=temp_id).first()
    if info:
        info.shop_id = info_dict.get('shop_id', '')
        info.shop_name = info_dict.get('shop_name', '')
        info.shop_address = info_dict.get('shop_address', '')
        info.shop_phone = info_dict.get('shop_phone', '')
        info.shop_openTime = info_dict.get('shop_openTime', '')
        info.shop_avgScore = info_dict.get('shop_avgScore', '')
        info.shop_avgPrice = info_dict.get('shop_avgPrice', '')
        info.shop_city = info_dict.get('shop_city', '')
        info.log_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    else:
        inset_data = shop(
            shop_id=info_dict.get('shop_id', ''),
            shop_name=info_dict.get('shop_name', ''),
            shop_address=info_dict.get('shop_address', ''),
            shop_phone=info_dict.get('shop_phone', ''),
            shop_openTime=info_dict.get('shop_openTime', ''),
            shop_avgScore=info_dict.get('shop_avgScore', ''),
            shop_avgPrice=info_dict.get('shop_avgPrice', ''),
            shop_city=info_dict.get('shop_city', ''),
            log_date=time.strftime('%Y-%m-%d', time.localtime(time.time()))
        )
        SQLsession.add(inset_data)
    SQLsession.commit()


# 写入顾客评论信息
def comment_db(info_dict):
    temp_id = info_dict['reviewId']
    # 判断是否已存在记录
    info = SQLsession.query(comment).filter_by(reviewId=temp_id).first()
    if info:
        info.shop_id = info_dict.get('shop_id', '')
        info.userId = info_dict.get('userId', '')
        info.userName = info_dict.get('userName', '')
        info.userScore = info_dict.get('userScore', '')
        info.comment = info_dict.get('comment', '')
        info.commentTime = info_dict.get('commentTime', '')
        info.merchantComment = info_dict.get('merchantComment', '')
        info.log_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    else:
        inset_data = comment(
            shop_id=info_dict.get('shop_id', ''),
            reviewId=info_dict.get('reviewId', ''),
            userId=info_dict.get('userId', ''),
            userName=info_dict.get('userName', ''),
            userScore=info_dict.get('userScore', ''),
            comment=info_dict.get('comment', ''),
            commentTime=info_dict.get('commentTime', ''),
            merchantComment=info_dict.get('merchantComment', ''),
            log_date=time.strftime('%Y-%m-%d', time.localtime(time.time()))
        )
        SQLsession.add(inset_data)
    SQLsession.commit()
