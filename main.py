from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
import redis
from pydantic import BaseModel

import models
from database import engine, get_db
from auth import get_password_hash, verify_password, create_access_token

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="CryptoWatch API - Baza i Auth")
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

class UserCreate(BaseModel):
    email: str
    password: str

@app.post("/register")
def register_user(user: UserCreate, db: Session = Depends(get_db)):
    db_user = db.query(models.User).filter(models.User.email == user.email).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Email juz istnieje")
    hashed_pw = get_password_hash(user.password)
    new_user = models.User(email=user.email, hashed_password=hashed_pw)
    db.add(new_user)
    db.commit()
    return {"message": "Uzytkownik zarejestrowany!"}

@app.post("/login")
def login(user: UserCreate, db: Session = Depends(get_db)):
    db_user = db.query(models.User).filter(models.User.email == user.email).first()
    if not db_user or not verify_password(user.password, db_user.hashed_password):
        raise HTTPException(status_code=401, detail="Zle dane")
    token = create_access_token(data={"sub": db_user.email})
    return {"access_token": token, "token_type": "bearer"}

@app.get("/price/{coin}")
def get_price_from_redis(coin: str):
    try:
        price = redis_client.get(f"price:{coin.upper()}")
        if not price:
            return {"message": "Brak ceny w cache. Kafka jeszcze nie wyslala danych."}
        return {"coin": coin.upper(), "price": price}
    except Exception:
        return {"message": "Brak polaczenia z Redisem. Upewnij sie, ze Docker dziala."}