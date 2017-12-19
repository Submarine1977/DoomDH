ADD_EXECUTABLE(ddhnode DoomDHNode.c)
TARGET_LINK_LIBRARIES(ddhnode pthread sqlite3 ${CMAKE_DL_LIBS})