ADD_EXECUTABLE(ddhnode DoomDHNode.c ../Sqlite/sqlite3.c)
TARGET_LINK_LIBRARIES(ddhnode pthread ${CMAKE_DL_LIBS})