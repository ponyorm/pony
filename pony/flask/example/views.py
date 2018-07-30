from .app import app
from .models import db
from flask import render_template, request, flash, redirect, abort
from flask_login import current_user, logout_user, login_user, login_required
from datetime import datetime
from pony.orm import flush

@app.route('/')
def index():
    users = db.User.select()
    return render_template('index.html', user=current_user, users=users)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        possible_user = db.User.get(login=username)
        if not possible_user:
            flash('Wrong username')
            return redirect('/login')
        if possible_user.password == password:
            possible_user.last_login = datetime.now()
            login_user(possible_user)
            return redirect('/')

        flash('Wrong password')
        return redirect('/login')
    else:
        return render_template('login.html')
    
@app.route('/reg', methods=['GET', 'POST'])
def reg():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        exist = db.User.get(login=username)
        if exist:
            flash('Username %s is already taken, choose another one' % username)
            return redirect('/reg')

        user = db.User(login=username, password=password)
        user.last_login = datetime.now()
        flush()
        login_user(user)
        flash('Successfully registered')
        return redirect('/')
    else:
        return render_template('reg.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Logged out')
    return redirect('/')