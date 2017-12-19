ADD_EXECUTABLE(ddhimportdh DoomDHImportDH.c)
TARGET_LINK_LIBRARIES(ddhimportdh pthread sqlite3 ${CMAKE_DL_LIBS})