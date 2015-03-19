
import pickle
import cPickle as c_pickle
import h5py
import shelve
import sqlite3

import os
import time
import matplotlib.pyplot as plt


import numpy as np

def save_by_pickle(data, dim):
    f = open("save/pickle/pickle" + str(dim) + ".pkl", 'wb')
    pickle.dump(data, f)
    f.close()

def load_by_pickle(dim):
    f = open("save/pickle/pickle" + str(dim) + ".pkl", 'rb')
    data = pickle.load(f)
    f.close()
    return data

def save_by_cpickle(data, dim):
    f = open("save/cpickle/cpickle" + str(dim) + ".pkl", 'wb')
    c_pickle.dump(data, f)
    f.close()

def load_by_cpickle(dim):
    f = open("save/cpickle/cpickle" + str(dim) + ".pkl", 'rb')
    data = c_pickle.load(f)
    f.close()
    return data


def save_by_npy(data, dim):
    np.save("save/numpy/numpy" + str(dim) + ".npy", data)

def load_by_npy(dim):
    data = np.load("save/numpy/numpy" + str(dim) + ".npy")
    return data

def save_by_hdf5(data, dim):
    f = h5py.File("save/h5py/h5py" + str(dim) + ".h5",'w')
    f.create_dataset("data",data=data)
    f.flush()
    f.close()

def load_by_hdf5(dim):
    f = h5py.File("save/h5py/h5py" + str(dim) + ".h5", "r")
    data = f["data"].value
    f.flush()
    f.close()
    return data

def save_by_shelve(data, dim):
    f = shelve.open("save/shelve/shelve" + str(dim))
    f["data"] = data
    f.close()

def load_by_shelve(dim):
    f = shelve.open("save/shelve/shelve" + str(dim))
    data = f["data"]
    f.close()
    return data

def prepare_sqlite():
    con = sqlite3.connect("save/sqlite/sqlite.db")
    try:
        sql = "DROP TABLE data_tb;"
        con.execute(sql)
    except:
        pass
    sql = "CREATE TABLE data_tb (dim INT PRIMARY KEY, data BLOB);"
    con.execute(sql)
    con.close()


def save_by_sqlite(data, dim):
    con = sqlite3.connect("save/sqlite/sqlite.db")
    sql = 'INSERT INTO data_tb VALUES(?, ?)'
    blob = buffer(data)
    con.execute(sql, (dim, blob))
    con.commit()
    con.close()

def load_by_sqlite(dim):
    con = sqlite3.connect("save/sqlite/sqlite.db")
    cur = con.cursor()
    sql = 'SELECT data FROM data_tb WHERE dim = ?'
    cur.execute(sql, (dim,))
    data = cur.fetchone()[0]
    return np.frombuffer(data).reshape(dim, dim)

def save_speed_test(my_func, dim):
    print str(my_func)
    print dim
    data = np.random.rand(dim, dim)
    start = time.time()
    my_func(data, dim)
    end = time.time()
    interval = end - start
    print "finished in %.5f sec" % interval
    return interval

def load_speed_test(my_func, dim):
    print str(my_func)
    print dim
    start = time.time()
    data = my_func(dim)
    end = time.time()
    interval = end - start
    print data.shape
    print "finished in %.5f sec" % interval
    return interval


min_dim = 100
max_dim = 3000
step_dim = 100

# create directories
modules = ["pickle","h5py","numpy", "cpickle", "shelve", "sqlite"]
for dir_name in modules:
    if not os.path.isdir("save/" + dir_name):
        print "creating directory: %s" % "save/" + dir_name
        os.mkdir("save/" + dir_name)

prepare_sqlite()

# save & load
save_pickle_times = [save_speed_test(save_by_pickle, i) for i in xrange(min_dim, max_dim + 1, step_dim)]
load_pickle_times = [load_speed_test(load_by_pickle, i) for i in xrange(min_dim, max_dim + 1, step_dim)]
save_npy_times = [save_speed_test(save_by_npy, i) for i in xrange(min_dim, max_dim + 1, step_dim)]
load_npy_times = [load_speed_test(load_by_npy, i) for i in xrange(min_dim, max_dim + 1, step_dim)]
save_h5py_times = [save_speed_test(save_by_hdf5, i) for i in xrange(min_dim, max_dim + 1, step_dim)]
load_h5py_times = [load_speed_test(load_by_hdf5, i) for i in xrange(min_dim, max_dim + 1, step_dim)]
save_shelve_times = [save_speed_test(save_by_shelve, i) for i in xrange(min_dim, max_dim + 1, step_dim)]
load_shelve_times = [load_speed_test(load_by_shelve, i) for i in xrange(min_dim, max_dim + 1, step_dim)]
save_cpickle_times = [save_speed_test(save_by_cpickle, i) for i in xrange(min_dim, max_dim + 1, step_dim)]
load_cpickle_times = [load_speed_test(load_by_cpickle, i) for i in xrange(min_dim, max_dim + 1, step_dim)]
save_sqlite_times = [save_speed_test(save_by_sqlite, i) for i in xrange(min_dim, max_dim + 1, step_dim)]
load_sqlite_times = [load_speed_test(load_by_sqlite, i) for i in xrange(min_dim, max_dim + 1, step_dim)]
# plot save time
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], save_pickle_times, "xr-", label="pickle")
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], save_npy_times, "xg-", label="numpy")
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], save_h5py_times, "xb-", label="h5py")
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], save_shelve_times, "xc-", label="shelve")
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], save_cpickle_times, "xm-", label="cpickle")
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], save_sqlite_times, "xy-", label="sqlite")
plt.legend(loc='upper left')
plt.title("Save Time")
plt.xlabel("numpy dimension")
plt.ylabel("time(second)")
plt.savefig("save_time.png")
plt.show()

# plot load time
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], load_pickle_times, "xr-", label="pickle")
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], load_npy_times, "xg-", label="numpy")
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], load_h5py_times, "xb-", label="h5py")
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], load_shelve_times, "xc-", label="shelve")
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], load_cpickle_times, "xm-", label="cpickle")
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], load_sqlite_times, "xy-", label="sqlite")
plt.legend(loc='upper left')
plt.title("Load Time")
plt.xlabel("numpy dimension")
plt.ylabel("time(second)")
plt.savefig("load_time.png")
plt.show()

# plot save time
# plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], save_pickle_times, "xr-", label="pickle")
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], save_npy_times, "xg-", label="numpy")
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], save_h5py_times, "xb-", label="h5py")
# plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], save_shelve_times, "xc-", label="shelve")
# plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], save_cpickle_times, "xm-", label="cpickle")
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], save_sqlite_times, "xy-", label="sqlite")
plt.legend(loc='upper left')
plt.title("Save Time")
plt.xlabel("numpy dimension")
plt.ylabel("time(second)")
plt.savefig("save_time2.png")
plt.show()

# plot load time
# plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], load_pickle_times, "xr-", label="pickle")
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], load_npy_times, "xg-", label="numpy")
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], load_h5py_times, "xb-", label="h5py")
# plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], load_shelve_times, "xc-", label="shelve")
# plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], load_cpickle_times, "xm-", label="cpickle")
plt.plot([i for i in xrange(min_dim, max_dim + 1, step_dim)], load_sqlite_times, "xy-", label="sqlite")
plt.legend(loc='upper left')
plt.title("Load Time")
plt.xlabel("numpy dimension")
plt.ylabel("time(second)")
plt.savefig("load_time2.png")
plt.show()
