function AutoList(callback) {
  this.setupCommands = function () {
    var me = this;
    me.commands_todo = [];
    var rows = $("#al_commands").val().split("\n");
    $("#main_table input.qcb").each(function () {
      var o = $(this);
      if (!o.is(":checked")) return;
      var tr = $(o.parents("tr").get(0));
      var q = o.attr("q");

      if (me.mode == "creator") {
        var cmd = { q: q, mode: "create" };
        var a = tr.find("a.pagelink").first();
        if (a.length === 0) return;
        cmd.page = decodeURIComponent(
          (a.attr("href") || "").replace(/^.+?\/wiki\//, ""),
        );
        me.commands_todo.push(cmd);
      }

      $.each(rows, function (k, row) {
        let cmd = { q: q };
        let m = row.match(/^\s*D(\S+):(.+)$/i);
        if (m != null) {
          cmd.mode = "desc";
          cmd.language = m[1];
          cmd.value = m[2].replace(/^"(.+?)"$/, "$1");
        } else {
          m = row.match(/^\s*-(P\d+)/i);
          if (m == null) {
            m = row.match(/^\s*(P\d+)\s*:\s*(Q\d+)\s*$/i);
            if (m != null) {
              cmd.mode = "add";
              cmd.prop = m[1];
              cmd.value = m[2];
            } else return;
          } else {
            // Delete property
            cmd.mode = "delete";
            cmd.prop = m[1];
            m = row.match(/^\s*-(P\d+)\s*:\s*(Q\d+)/i);
            if (m != null) cmd.value = m[2];
          }
        }
        me.commands_todo.push(cmd);
      });
    });
  };

  this.initializeAutoListBox = function () {
    var me = this;
    var h = "";
    var p = getUrlVars();
    if (me.mode == "creator") {
      h += "<div class='autolist_subbox'><div tt='al_creator_mode'></div></div>";
    }
    h += "<div class='autolist_subbox'>";
    h +=
      "<button id='al_do_check_all' class='btn btn-outline-secondary btn-sm' tt='al_all' style='width:100%'></button><br/>";
    h +=
      "<button id='al_do_check_none' class='btn btn-outline-secondary btn-sm' tt='al_none' style='width:100%'></button><br/>";
    h +=
      "<button id='al_do_check_toggle' class='btn btn-outline-secondary btn-sm' tt='al_toggle' style='width:100%'></button><br/>";
    h += "</div>";
    h += "<div class='autolist_subbox'>";
    h +=
      "<textarea id='al_commands' tt_placeholder='al_commands_ph' rows=3 style='padding:2px;width:200px'>" +
      (p.statementlist || "") +
      "</textarea><br/>";
    h +=
      "<button id='al_start_qs' class='btn btn-outline-success btn-sm' tt='al_start_qs'></button>";
    h +=
      "<form style='display:none' id='qs_form' action='https://quickstatements.toolforge.org/api.php' method='post' target='_blank'>";
    h += "<input type='hidden' name='action' value='import' />";
    h += "<input type='hidden' name='format' value='v1' />";
    h += "<input type='hidden' name='temporary' value='1' />";
    h += "<input type='hidden' name='openpage' value='1' />";
    h +=
      "<input type='hidden' name='site' value='" +
      (autolist_wiki_server == "commons.wikimedia.org"
        ? "commons"
        : "wikidata") +
      "' />";
    h +=
      "<textarea type='hidden' id='qs_commands' name='data'></textarea><button name='yup'></button></form>";
    h += "</div>";
    $("#autolist_box").html(h);
    tt.updateInterface($("#autolist_box"));

    if (typeof p.al_commands != "undefined") {
      $("#al_commands").val(p.al_commands);
      me.commandsHaveChanged();
    }

    me.setInterfaceLanguage(interface_language);

    $("#al_start_qs").click(function (e) {
      e.preventDefault();
      me.setupCommands();
      let qs_commands = [];
      let entity_letter =
        autolist_wiki_server == "www.wikidata.org" ? "Q" : "M";
      $.each(me.commands_todo, function (dummy, cmd) {
        let qs = "";
        if (cmd.mode == "create") {
          qs = "CREATE";
          qs += "||LAST|S" + output_wiki + '|"' + cmd.page + '"';
          let m = output_wiki.match(/^([a-z-]+)wiki$/);
          if (m !== null) {
            let lang = m[1] == "commons" ? interface_language : m[1];
            let the_label = $.trim(
              cmd.page.replace(/_/g, " ").replace(/\s*\(.+?\)\s*/, " "),
            ).replace(/^\S+:/, "");
            qs += "||LAST|L" + lang + '|"' + the_label + '"';
          }
        } else if (cmd.mode == "desc") {
          qs = "LAST|D" + cmd.language + '|"' + cmd.value + '"';
        } else {
          if (cmd.mode == "delete") qs = "-";
          if (/^create_item_/.test(cmd.q)) qs += "LAST";
          else qs += entity_letter + cmd.q;
          qs += "|" + cmd.prop;
          if (typeof cmd.value != "undefined") qs += "|" + cmd.value;
        }

        qs_commands.push(qs);
      });
      let s = qs_commands.join("||");
      $("#qs_commands").val(s);
      $("#qs_form").submit();
    });

    $("#al_do_check_all").click(function (e) {
      e.preventDefault();
      $("#main_table input.qcb").prop("checked", true);
    });

    $("#al_do_check_none").click(function (e) {
      e.preventDefault();
      $("#main_table input.qcb").prop("checked", false);
    });

    $("#al_do_check_toggle").click(function (e) {
      e.preventDefault();
      $("#main_table input.qcb").each(function () {
        var o = $(this);
        o.prop("checked", !o.prop("checked"));
      });
    });

    $("#al_commands").keyup(function () {
      me.commandsHaveChanged();
    });
  };

  this.commandsHaveChanged = function () {
    $('#main_form input[name="al_commands"]').remove();
    $("#main_form").append("<input type='hidden' name='al_commands' />");
    var v = $("#al_commands").val();
    $('#main_form input[name="al_commands"]').val(v);
    $("#permalink a").each(function () {
      var a = $(this);
      var href = a.attr("href");
      href = href.replace(/&al_commands=[^&]+/, "");
      href += "&al_commands=" + encodeURIComponent(v);
      href = href.replace(/&{2,}/, "&");
      a.attr({ href: href });
    });
  };

  this.setInterfaceLanguage = function (l) {
    tt.setLanguage(l);
  };

  this.addCheckLinks = function () {
    var me = this;
    if (me.mode != "creator") return;
    $("#main_table tbody tr").each(function () {
      var tr = $(this);
      var a = tr.find("a.pagelink").first();
      if (a.length === 0) return;
      var td = a.closest("td");
      var page = (a.attr("href") || "")
        .replace(/^.+?\/wiki\//, "")
        .replace(/_/, " ");
      td.append(
        " <span class='pull-xs-right smaller'>[<a tt='check_wd' target='_blank'></a>]</span>",
      );
      tt.updateInterface(td);
      td.find('a[tt="check_wd"]').attr({
        href:
          "https://wikidata-todo.toolforge.org/duplicity.php?wiki=" +
          output_wiki +
          "&norand=1&page=" +
          page,
      });
    });
  };

  var me = this;
  if (
    $("#autolist_box").length === 0 ||
    $("#main_table input.qcb").length === 0
  ) {
    // Don't bother
    if (typeof callback != "undefined") callback();
    return;
  }

  $(document).ready(function () {
    me.mode = $("#autolist_box").attr("mode");
    me.commands_todo = [];
    me.addCheckLinks();
    me.initializeAutoListBox();
  });
}
