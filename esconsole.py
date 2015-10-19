import urwid
import elasticsearch
import sys
import re
import time
import threading

# Run /_cat/health every this many seconds
HEALTH_UPDATE_FREQ=3



class MultiSelectListWidget(urwid.WidgetWrap):
    """ This widget implements generic selection and filtering on a list of passed in data. """
    def __init__(self, els):
        self.els = els
        buttons = []
        for e in els:
            buttons.append(urwid.AttrMap(urwid.Button(e), None, focus_map=None))
        self.listbox = urwid.ListBox(urwid.SimpleFocusListWalker(buttons))
        super(MultiSelectListWidget, self).__init__(self.listbox)

    def selected(self):
        result = []
        for el in self.listbox.body:
            if None in el.attr_map and el.attr_map[None] == 'reversed':
                result.append(el.original_widget.label)
        return result

    def item_under_cursor(self):
        return self.listbox.focus.original_widget.label

    def keypress(self, size, key):
        if key == "v":
            # toggle selection
            if None in self.listbox.focus.attr_map and self.listbox.focus.attr_map[None] == 'reversed':
                self.listbox.focus.set_attr_map({'reversed': None})
            else:
                self.listbox.focus.set_attr_map({None: 'reversed'})
        elif key == 'c':
            # clear all
            for el in self.listbox.body:
                if None in el.attr_map and el.attr_map[None] == 'reversed':
                    el.set_attr_map({'reversed': None})
        # vi style up/down
        elif key == 'k':
            return super(MultiSelectListWidget, self).keypress(size, 'up')
        elif key == 'j':
            return super(MultiSelectListWidget, self).keypress(size, 'down')
        else:
            return super(MultiSelectListWidget, self).keypress(size, key)

class CatIndicesLine(object):
        def __init__(self, line):
            hdrs = ['health', 'status', 'index', 'pri', 'rep', 'docs_count', 'docs_deleted', 'store_size', 'pri_store_size']
            int_fields = set(['pri', 'rep', 'docs_count', 'docs_deleted'])
            # es 1.7 headers ^
            # example lines
            # green  open   2015-10-10t00:00:00.000z   5   0          0            0       720b           720b 
            #        close  2015-08-11t00:00:00.000z
            for h in hdrs:
                setattr(self, h, None)
            fields = re.split(" +", line.strip())
            if len(fields) == 2:
                self.status, self.index = fields
            else:
                for h,f in zip(hdrs, fields):
                    if h in int_fields:
                        val = int(f)
                    else:
                        val = f
                    setattr(self, h, val)
        
        
class IndicesListWidget(urwid.WidgetWrap):
    """ This widget displays the Elasticsearch Cat Indices result in a sorted way """
    def __init__(self, main, es):
        self.es = es
        self.main = main
        indices = self.sort_indices(es.cat.indices().rstrip().split("\n"))
        self.multilistbox = MultiSelectListWidget(indices)
        super(IndicesListWidget, self).__init__(self.multilistbox)

    def sort_indices(self, indices):
        ind_objs = []
        for i in indices:
            ind_objs.append((i, CatIndicesLine(i).index))

        ind_objs = sorted(ind_objs, key=lambda x: x[1])
        return [i[0] for i in ind_objs]

    def keypress(self, size, key):
        if key == 'D':
            self.delete_selected_indices()
        elif key == 'A':
            self.append_index_after_index_under_cursor()
        else:
            return super(IndicesListWidget, self).keypress(size, key)

    def selected(self):
        return self.multilistbox.selected()

    def delete_selected_indices(self):
        self.main.popup_yes_no("Delete %d indices?" % (len(self.selected())), self.delete_selected_indices_answer)
        
    def delete_selected_indices_answer(self, answer):
        if answer != 'y':
            return

        indices = [CatIndicesLine(l) for l in self.selected()]
        for i in indices:
            self.es.indices.delete(i.index)

        # TODO - validate they are deleted
        self.main.refresh()

    def index_under_cursor(self):
        return self.multilistbox.item_under_cursor()

    def append_index_after_index_under_cursor(self):
        index_under_cursor = CatIndicesLine(self.index_under_cursor())

        # Come up with suggestion for new index name
        # This will break for non date type indices
        ts, msec = index_under_cursor.index.split(".")
        msec = int(msec.rstrip("z"))
        suggestion = "%s.%03dz" % (ts, msec+1)

        self.main.popup(IndexInputPopup("Create index after %s" % index_under_cursor.index, suggestion, index_under_cursor.pri, index_under_cursor.rep, self.create_index))

    def create_index(self, cancel, index, primaries, replicas):
        if cancel:
            return
        self.es.indices.create(index=index,
            body = {
                "settings": {
                    "index": {
                        "number_of_shards": primaries,
                        "number_of_replicas": replicas
                    }
                }
            }
        )

        self.main.refresh()
        # TODO - make sure create actually worked

class NumberEdit(urwid.Edit):
    def __init__(self, caption, default):
        super(NumberEdit, self).__init__(caption, edit_text=str(default))

    def valid_char(self, ch):
        return ch in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']

    def value(self):
        return int(self.edit_text)
        
class IndexInputPopup(urwid.WidgetWrap):
    """ A popup that helps create an index """
    def __init__(self, msg, default_index_name, default_primaries, default_replicas, callback):
        self.callback = callback        

        self.index_name = urwid.Edit(caption   ='Index name : ', edit_text=default_index_name)
        self.primaries = NumberEdit(caption='Primaries  : ', default=default_primaries)
        self.replicas = NumberEdit(caption='Replicas   : ', default=default_replicas)

        pile = urwid.Pile([
            urwid.Text(msg),
            urwid.Divider('-'),
            self.index_name,
            self.primaries,
            self.replicas,
            urwid.Divider(' '),
            urwid.Text('(up/down keys to move between inputs, enter to create, esc to cancel)')
        ])
        frame = urwid.Frame(urwid.LineBox(urwid.Filler(pile)))
        super(IndexInputPopup, self).__init__(frame)

    def show_popup(self, base, loop):
        self.base = base
        self.loop = loop
        self.overlay = urwid.Overlay(self, self.base, 'center', 60, 'middle', 10)
    
        self.loop.widget = self.overlay

    def keypress(self, size, key):
        if key == 'esc':
            self.hide()
            self.call_callback(True)
        elif key == 'enter':
            self.hide()
            self.call_callback(False)
        else:
            return super(IndexInputPopup, self).keypress(size, key)

    def call_callback(self, cancel):
        self.callback(cancel, self.index_name.edit_text, self.primaries.value(), self.replicas.value())

    def hide(self):
        self.loop.widget = self.base
            
            

        
        

class HealthDisplayWidget(urwid.WidgetWrap):
    """ Display cluster health on an interval """
    def __init__(self, health_watcher):
        self.health_watcher = health_watcher
        self.textbox = urwid.Text("loading...")
        filler = urwid.Filler(self.textbox)
        super(HealthDisplayWidget, self).__init__(filler)

    def update_health(self, health, loop):
        self.textbox.set_text(health)

    def update(self, loop, user_data):
        self.textbox.set_text(self.health_watcher.health)
        loop.set_alarm_in(HEALTH_UPDATE_FREQ, self.update)


class ElasticsearchHealthWatchThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.es = elasticsearch.Elasticsearch()
        self.daemon = True
    
    def run(self):
        while True:
            self.health = self.es.cat.health(v=True).rstrip()
            time.sleep(HEALTH_UPDATE_FREQ)



class YesNoPopup(urwid.WidgetWrap):
    def __init__(self, msg, base, loop, callback):
        
        self.base = base
        self.overlay = urwid.Overlay(urwid.Frame(urwid.LineBox(urwid.Filler(urwid.Text(msg)))), base, 'center', len(msg) + 3, 'middle', 3)
        self.loop = loop
        self.callback = callback
    
        super(YesNoPopup, self).__init__(self.overlay)
        self.loop.widget = self

    def keypress(self, size, key):
        if key.upper() == 'Y':
            self.cancel()
            self.callback('y')
        elif key.upper() == 'N':
            self.cancel()
            self.callback('n')
        elif key == 'esc':
            self.cancel()
            self.callback('n')
        else:
            return super(YesNoPopup, self).keypress(size, key)


    def cancel(self):
        self.loop.widget = self.base

class HelpPopupWidget(urwid.WidgetWrap):
    """ Show help text """

    def __init__(self, base, loop):
        help_text = """                               ES CONSOLE

--------------------------------------------------------------------------------

                                  MOVING

    h, up arrow         up
    j, down arrow       down
    page down           page down
    page up             page up
    g                   go to first line (not implemented)
    G                   go to last line (not implemented)
--------------------------------------------------------------------------------

                                   MISC

    space               refresh display
    esc                 cancel popups
    q                   quit
--------------------------------------------------------------------------------

                                 SELECTING

    v                   mark for multi-operation
    c                   clear selections
    f                   filter (not implemented)                 
    /                   search (not implemented)
--------------------------------------------------------------------------------

                                OPERATIONS

    D                   delete selected indices
    A                   append new index after index under cursor
    C                   create index (not implemented)
--------------------------------------------------------------------------------

                        (press any key to close)
"""

    
        self.base = base
        self.overlay = urwid.Overlay(urwid.Frame(urwid.LineBox(urwid.Filler(urwid.Text(help_text)))), base, 'center', 82, 'middle', len(help_text.split("\n")) + 3)
        self.loop = loop
    
        super(HelpPopupWidget, self).__init__(self.overlay)
        self.loop.widget = self

    def keypress(self, size, key):
        # cancel popup on any key
        self.loop.widget = self.base
    


class MainScreenWidget(urwid.WidgetWrap):
    def __init__(self):
        health_updater = ElasticsearchHealthWatchThread()
        health_updater.start()
        self.is_popup = False


        self.health_display = HealthDisplayWidget(health_updater)

        self.es = elasticsearch.Elasticsearch()

        self.main_pile = urwid.Pile([
            (2, self.health_display),
            urwid.Divider('-'),
            (self.get_screen_rows() - 3, IndicesListWidget(self, self.es))
        ], focus_item=2)
    
        main_filler = urwid.Filler(self.main_pile, valign='top', height='pack')

        super(MainScreenWidget, self).__init__(main_filler)

    def get_screen_rows(self):
        cols, rows = urwid.raw_display.Screen().get_cols_rows()
        return rows

    def init_loop(self, loop):
        self.loop = loop
        loop.set_alarm_in(0, self.start_update_health)

    def start_update_health(self, loop, userdata):
        self.health_display.update(loop, userdata)

    def popup_yes_no(self, msg, callback):
        msg = "%s (y/n)" % (msg)
        YesNoPopup(msg, self, self.loop, callback)

    def popup(self, popup_widget):
        popup_widget.show_popup(self, self.loop)

    def keypress(self, size, key):
        if key == 'q':
            raise urwid.ExitMainLoop()
        elif key == ' ':
            self.refresh()
        elif key == '?':
            HelpPopupWidget(self, self.loop)
        else:    
            return super(MainScreenWidget, self).keypress(size, key)

    def refresh(self):
        # poor man's refresh
        self.main_pile.contents[2] =  (IndicesListWidget(self, self.es), self.main_pile.contents[2][1])


main_screen = MainScreenWidget()

loop = urwid.MainLoop(main_screen, palette=[('reversed', 'standout', '')])

main_screen.init_loop(loop)

loop.run()
