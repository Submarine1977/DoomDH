ADD_EXECUTABLE(ddhimportdh DoomDHImportDH.c ../Sqlite/sqlite3.c)
TARGET_LINK_LIBRARIES(ddhimportdh pthread ${CMAKE_DL_LIBS})