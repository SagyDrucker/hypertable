/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <iostream>
#include <vector>

extern "C" {
#include <readline/readline.h>
#include <readline/history.h>
}

#include "Common/InteractiveCommand.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "Hypertable/Lib/Manager.h"

#include "CommandCreateTable.h"
#include "CommandGetSchema.h"

using namespace hypertable;
using namespace std;

namespace {

  char *line_read = 0;

  char *rl_gets () {

    if (line_read) {
      free (line_read);
      line_read = (char *)NULL;
    }

    /* Get a line from the user. */
    line_read = readline ("hypertable> ");

    /* If the line has any text in it, save it on the history. */
    if (line_read && *line_read)
      add_history (line_read);

    return line_read;
  }

  const char *usage[] = {
    "usage: hypertable [OPTIONS]",
    "",
    "OPTIONS:",
    "  --config=<file>  Read configuration from <file>.  The default config file is",
    "                   \"conf/hypertable.cfg\" relative to the toplevel install directory",
    "  --help             Display this help text and exit",
    ""
    "This is a command line interface to a Hypertable cluster.",
    (const char *)0
  };
  
}


/**
 *
 */
int main(int argc, char **argv) {
  const char *line;
  size_t i;
  string configFile = "";
  vector<InteractiveCommand *>  commands;
  Manager *manager = 0;

  System::Initialize(argv[0]);

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--config=", 9))
      configFile = &argv[i][9];
    else
      Usage::DumpAndExit(usage);
  }

  if (configFile == "")
    configFile = System::installDir + "/conf/hypertable.cfg";

  Manager::Initialize(configFile);

  manager = Manager::Instance();

  commands.push_back( new CommandCreateTable(manager) );
  commands.push_back( new CommandGetSchema(manager) );

  cout << "Welcome to the Hypertable command interpreter." << endl;
  cout << "Type 'help' for a description of commands." << endl;
  cout << endl << flush;

  using_history();
  while ((line = rl_gets()) != 0) {

    if (*line == 0)
      continue;

    for (i=0; i<commands.size(); i++) {
      if (commands[i]->Matches(line)) {
	commands[i]->ParseCommandLine(line);
	commands[i]->run();
	break;
      }
    }

    if (i == commands.size()) {
      if (!strcmp(line, "quit") || !strcmp(line, "exit"))
	exit(0);
      else if (!strcmp(line, "help")) {
	cout << endl;
	for (i=0; i<commands.size(); i++) {
	  Usage::Dump(commands[i]->Usage());
	  cout << endl;
	}
      }
      else
	cout << "Unrecognized command." << endl;
    }

  }

  return 0;
}