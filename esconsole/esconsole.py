import urwid
import elasticsearch
import sys
import re
import time
import threading
import datetime
import itertools

# Run /_cat/health every this many seconds
HEALTH_UPDATE_FREQ=3

debug = False
if debug:
    debug_fh = open("debug.txt", "w")
def debug(s):
    global debug_fh
    debug_fh.write(str(s))
    debug_fh.write("\n")
    debug_fh.flush()

def byte_format(num):
    if num is None or num == "":
        return ""
    num = float(num)
    for suffix in ['b', 'kb', 'mb', 'gb', 'tb', 'pb']:
        if num < 1000:
            if suffix == 'b':
                return "%6d%s" % (num, suffix)
            formatted = "%5.1f" % (num)
            whole, decimal = formatted.split(".")
            if decimal == "0":
                return "  %s%s" % (whole, suffix)
            return "%s%s" % (formatted, suffix)
        num = num / 1000

class MultiSelectListWidget(urwid.WidgetWrap):
    """ This widget implements generic selection and filtering on a list of passed in data. """
    def __init__(self, listdata):
        # listdata should be array-ish and also implement a .headers property
        self.listdata = listdata

        # determine how wide each column should be
        col_width = {}
        for h in self.listdata.headers:
            col_width[h] = len(h)
        for row in self.listdata:
            for h in self.listdata.headers:
                val = row.format(h)
                if len(str(val)) > col_width[h]:
                    col_width[h] = len(str(val))


        # format each row
        buttons = []
        for row in self.listdata:
            el = []
            for h in self.listdata.headers:
                padding = col_width[h] - len(str(row.format(h)))
                el.append(str(row.format(h)) + (" " * padding))
            buttons.append(urwid.AttrMap(urwid.Button(" | ".join(el)), None, focus_map=None))

        # format headers
        hdr_txt = []
        for h in self.listdata.headers:
            padding = col_width[h] - len(h)
            hdr_txt.append(h + " " * padding)

        # reverse video for headers
        header_widget = urwid.AttrMap(urwid.Text("  " + "   ".join(hdr_txt)), 'reversed')
        self.listbox = urwid.ListBox(urwid.SimpleFocusListWalker(buttons))
        pile = urwid.Pile([
            ('pack', header_widget),
            ('weight', 1, self.listbox)
        ])

        super(MultiSelectListWidget, self).__init__(pile)

    def filter(self, filter_text):
        if filter_text == "":
            filter_text = ".*"
        filter_re = re.compile(filter_text)


    def selected(self):
        result = []
        i = 0
        for el in self.listbox.body:
            if None in el.attr_map and el.attr_map[None] == 'reversed':
                result.append(i)
            i += 1
        return result

    def item_under_cursor(self):
        return self.listbox.focus_position

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
        elif key == 'g':
            self.listbox.set_focus(0, 'below')
            return super(MultiSelectListWidget, self).keypress(size, None)
        elif key == 'G':
            self.listbox.set_focus(len(self.listdata) - 1, 'above')
            return super(MultiSelectListWidget, self).keypress(size, None)
        else:
            return super(MultiSelectListWidget, self).keypress(size, key)

class CatIndicesResponseLine(object):
        def __init__(self, line):
            self.line = line
            hdrs = ['health', 'status', 'index', 'pri', 'rep', 'docs_count', 'docs_deleted', 'store_size', 'pri_store_size']
            int_fields = set(['pri', 'rep', 'docs_count', 'docs_deleted', 'store_size', 'pri_store_size'])
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

        def __repr__(self):
            return self.line

class CatIndicesResponse(object):
    """ Wrap Cat Indices Responses """
    def __init__(self, cat_indices_result):
        self.headers = ['health', 'status', 'index', 'pri', 'rep', 'docs_count', 'docs_deleted', 'store_size', 'pri_store_size']
        self.indices = []

        for line in cat_indices_result.rstrip().split("\n"):
            self.indices.append(CatIndicesResponseLine(line))

        self.indices = sorted(self.indices, key=lambda x: x.index)

    def __len__(self):
        return len(self.indices)

    def __getitem__(self, ndx):
        return self.indices[ndx]

class CatSegmentsResponse(object):
    """ Wrap Cat Segments Responses """
    def __init__(self, cat_segments_result):
        self.headers = ['index', 'shard', 'prirep', 'ip', 'segment', 'generation', 'docs_count', 'docs_deleted', 'size', 'size_memory', 'committed', 'searchable', 'version', 'compound']
        self.segments = []

        for line in cat_segments_result.rstrip().split("\n"):
            self.segments.append(CatSegmentsResponseLine(line))

    def __len__(self):
        return len(self.segments)

    def __getitem__(self, ndx):
        return self.segments[ndx]

class CatSegmentsResponseLine(object):
        def __init__(self, line):
            self.line = line
            hdrs = ['index', 'shard', 'prirep', 'ip', 'segment', 'generation', 'docs_count', 'docs_deleted', 'size', 'size_memory', 'committed', 'searchable', 'version', 'compound']
            int_fields = set(['shard', 'generation', 'docs_count', 'docs_deleted'])
            # es 1.7 headers ^
            # example
            # index                    shard prirep ip           segment generation docs.count docs.deleted size size.memory committed searchable version compound
            # 2015-10-05t00:00:00.000z 0     p      192.168.1.65 _1               1          1            0  2kb        3298 true      true       4.10.4  false

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

        def __repr__(self):
            return self.line

class IndexInfo(object):
    """ Wraps CatIndicesResponseLine and provides additional info """
    def __init__(self, cat_indices_info):
        self.cat_indices_info = cat_indices_info
        self.cat_segments_info = []
        self.prev_state = None

    def set_cat_segments_info(self, cat_segments_info):
        self.cat_segments_info = cat_segments_info

    def set_prev_state(self, prev_state):
        self.prev_state = prev_state

    def format(self, attr):
        # format field names
        if attr in ['pri_store_size', 'store_size']:
            return byte_format(getattr(self, attr))
        return getattr(self, attr)

    @property
    def age(self):
        # return age in days
        age_groups = re.match(r"^(\d+-\d+-\d+t\d+:\d+:\d+).(\d+z)$", self.cat_indices_info.index)
        if age_groups is None:
            return -1

        tstamp, msec = age_groups.group(1,2)
        try:
            date = datetime.datetime.strptime(tstamp, "%Y-%m-%dt%H:%M:%S")
            delta = datetime.datetime.now() - date
            return delta.days
        except Exception as e:
            return -1

    @property
    def segments(self):
        pri_segs = [s for s in self.cat_segments_info if s.prirep == 'p' and s.committed == 'true']
        unique_segs_per_shard = set()

        for k, g in itertools.groupby(pri_segs, key=lambda x:x.shard):
            unique_segs_per_shard.add(len(list(g)))

        segs_per_shard = sorted(list(unique_segs_per_shard))
        if len(segs_per_shard) == 0:
            return ""
        elif len(segs_per_shard) == 1:
            return str(segs_per_shard[0])
        else:
            return "%d - %d" % (segs_per_shard[0], segs_per_shard[-1])

    @property
    def hot(self):
        if not self.prev_state:
            return "?"

        if self.prev_state.docs_count != self.docs_count:
            return "hot"
        return ""

    @property
    def merging(self):
        if not self.prev_state:
            return "?"

        if (self.prev_state.docs_count == self.docs_count
                and self.prev_state.pri_store_size != self.pri_store_size):
            return "merging"

        return ""

    @property
    def replicating(self):
        if not self.prev_state:
            return "?"
        if (self.prev_state.docs_count == self.docs_count
                and self.prev_state.pri_store_size == self.pri_store_size
                and self.prev_state.store_size != self.store_size):
            return "rep"

        return ""


    # route other attrs through cat_indices_info
    def __getattr__(self, attr):
        return getattr(self.cat_indices_info, attr)

    def __repr__(self):
        return str(self.cat_indices_info)

class IndicesInfo(object):
    """ Adds cat indices plus some other stuff """
    def __init__(self, cat_indices_response, cat_segments_response):
        self.cat_indices_response = cat_indices_response
        self.cat_segments_response = cat_segments_response
        self.index_infos = [IndexInfo(i) for i in self.cat_indices_response]

        # Merge in cat segments data
        index_segments = {}
        for k, g in itertools.groupby(self.cat_segments_response, key=lambda x:x.index):
            index_segments[k] = list(g)
        for i in self.index_infos:
            if i.index in index_segments:
                i.set_cat_segments_info(index_segments[i.index])

    @property
    def headers(self):
        return ['health', 'status', 'index', 'pri', 'rep', 'docs_count', 'store_size', 'pri_store_size', 'age', 'segments', 'hot', 'merging']

    def __len__(self):
        return len(self.index_infos)

    def __getitem__(self, ndx):
        return self.index_infos[ndx]


class IndicesListWidget(urwid.WidgetWrap):
    """ This widget displays the Elasticsearch Cat Indices result in a sorted way """
    def __init__(self, main, es, prev_state=None):
        self.es = es
        self.main = main
        self.indices_info = IndicesInfo(CatIndicesResponse(self.es.cat.indices(bytes='b')), CatSegmentsResponse(self.es.cat.segments()))
        self.filter_text = ""

        if prev_state:
            cur_state = {}
            for i in self.indices_info:
                cur_state[i.index] = i
            for i in prev_state.indices_info:
                if i.index in cur_state:
                    cur_state[i.index].set_prev_state(i)

        self.multilistbox = MultiSelectListWidget(self.indices_info)
        super(IndicesListWidget, self).__init__(self.multilistbox)

    def keypress(self, size, key):
        if key == 'D':
            self.delete_selected_indices()
        elif key == 'A':
            self.append_index_after_selected_index()
        elif key == 'O':
            self.optimize_selected_indices()
        elif key == 'R':
            self.replicate_selected_indices()
        elif key == ' ':
            self.main.refresh()
        elif key == '/':
            pass
        else:
            return super(IndicesListWidget, self).keypress(size, key)

    def selected(self):
        return self.multilistbox.selected()

    def filter(self):
        self.main.popup(SingleTextInputPopup("Enter filter text (python compatible regex)", 'Regex : ', self.filter_text, self.filter_answer))

    def filter_answer(self, cancel, filter_text):
        if cancel:
            return
        self.multilistbox.filter(filter_text)

    def delete_selected_indices(self):
        self.main.popup_yes_no("Delete %d indices?" % (len(self.selected())), self.delete_selected_indices_answer)

    def delete_selected_indices_answer(self, answer):
        if answer != 'y':
            return

        indices = [self.indices_info[ndx] for ndx in self.selected()]
        for i in indices:
            self.es.indices.delete(i.index)

        # TODO - validate they are deleted
        self.main.refresh()

    def optimize_selected_indices(self):
        self.main.popup(SingleNumberInputPopup("Optimize %d indices" % (len(self.selected())), "Max Segments : ", 10, self.optimize_selected_indices_answer))

    def optimize_selected_indices_answer(self, cancel, max_num_segments):
        if cancel:
            return

        indices = [self.indices_info[ndx] for ndx in self.selected()]
        for i in indices:
            self.es.indices.optimize(i.index, max_num_segments=max_num_segments, wait_for_merge=False)
            # dirty trick - manually change store size so it will show up as merging on refresh
            i.pri_store_size = "optimizing"

        self.main.refresh()

    def replicate_selected_indices(self):
        if len(self.selected()) == 0:
            return

        default_reps = self.indices_info[self.selected()[0]].rep
        self.main.popup(SingleNumberInputPopup("Change replicas on %d indices" % (len(self.selected())), 'Replicas : ', default_reps, self.replicate_selected_indices_answer))

    def replicate_selected_indices_answer(self, cancel, replicas):
        if cancel:
            return

        indices = [self.indices_info[ndx] for ndx in self.selected()]
        for i in indices:
            self.es.indices.put_settings(index=i.index, body={
                "index": {
                    "number_of_replicas": replicas
                }
            })

        # TODO - make sure it worked
        self.main.refresh()

    def index_under_cursor(self):
        return self.indices_info[self.multilistbox.item_under_cursor()]

    def append_index_after_selected_index(self):
        indices = [self.indices_info[ndx] for ndx in self.selected()]
        if len(indices) != 1:
            return

        index = indices[0]

        # Come up with suggestion for new index name
        # This will break for non date type indices
        ts, msec = index.index.split(".")
        msec = int(msec.rstrip("z"))
        suggestion = "%s.%03dz" % (ts, msec+1)

        self.main.popup(IndexInputPopup("Create index after %s" % index.index, suggestion, index.pri, index.rep, self.create_index))

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


class SingleTextInputPopup(urwid.WidgetWrap):
    """ Asks for a single string """

    def __init__(self, title, caption, default, callback):
        self.callback = callback
        self.edit_box = urwid.Edit(caption=caption, edit_text=default)

        pile = urwid.Pile([
            urwid.Text(title),
            urwid.Divider('-'),
            self.edit_box,
            urwid.Divider(' '),
            urwid.Text('(enter to accept, esc to cancel)')
        ])

        frame = urwid.Frame(urwid.LineBox(urwid.Filler(pile)))
        super(SingleTextInputPopup, self).__init__(frame)

    def keypress(self, size, key):
        if key == 'esc':
            self.hide()
            self.call_callback(True)
        elif key == 'enter':
            self.hide()
            self.call_callback(False)
        else:
            return super(SingleTextInputPopup, self).keypress(size, key)

    def show_popup(self, base, loop):
        self.base = base
        self.loop = loop
        self.overlay = urwid.Overlay(self, self.base, 'center', 60, 'middle', 7)
        self.loop.widget = self.overlay

    def call_callback(self, cancel):
        self.callback(cancel, self.edit_box.edit_text)

    def hide(self):
        self.loop.widget = self.base

class SingleNumberInputPopup(urwid.WidgetWrap):
    """ Asks for a single number """

    def __init__(self, title, caption, default, callback):
        self.callback = callback
        self.edit_box = NumberEdit(caption=caption, default=default)

        pile = urwid.Pile([
            urwid.Text(title),
            urwid.Divider('-'),
            self.edit_box,
            urwid.Divider(' '),
            urwid.Text('(enter to accept, esc to cancel)')
        ])

        frame = urwid.Frame(urwid.LineBox(urwid.Filler(pile)))
        super(SingleNumberInputPopup, self).__init__(frame)

    def keypress(self, size, key):
        if key == 'esc':
            self.hide()
            self.call_callback(True)
        elif key == 'enter':
            self.hide()
            self.call_callback(False)
        else:
            return super(SingleNumberInputPopup, self).keypress(size, key)

    def show_popup(self, base, loop):
        self.base = base
        self.loop = loop
        self.overlay = urwid.Overlay(self, self.base, 'center', 60, 'middle', 7)
        self.loop.widget = self.overlay

    def call_callback(self, cancel):
        self.callback(cancel, self.edit_box.value())

    def hide(self):
        self.loop.widget = self.base

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

    j, down arrow       down
    k, up arrow         up
    page down           page down
    page up             page up
    g                   go to first line
    G                   go to last line
--------------------------------------------------------------------------------

                                   MISC

    space               refresh display
    esc                 cancel popups
    q                   quit
--------------------------------------------------------------------------------

                                 SELECTING

    v                   mark for multi-operation
    c                   clear selections
    /                   filter (not implemented)
--------------------------------------------------------------------------------

                                OPERATIONS

    D                   delete selected indices
    A                   append new index after selected index
    O                   optimize selected indices
    R                   change # replicas on selected indices
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
        elif key == '?':
            HelpPopupWidget(self, self.loop)
        else:
            return super(MainScreenWidget, self).keypress(size, key)

    def refresh(self):
        # poor man's refresh
        self.prev_indices_list, size  = self.main_pile.contents[2]
        self.main_pile.contents[2] =  (IndicesListWidget(self, self.es, self.prev_indices_list), size)


if __name__ == "__main__":
    main_screen = MainScreenWidget()

    loop = urwid.MainLoop(main_screen, palette=[('reversed', 'standout', '')])

    main_screen.init_loop(loop)

    loop.run()
