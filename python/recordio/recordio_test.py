import unittest

import recordio
import cPickle as pickle
import md5

class TestStringMethods(unittest.TestCase):
    def test_write_read(self):
        w = recordio.writer("/tmp/record_0")
        w.write(pickle.dumps("1"))
        w.write("2")
        w.write("")
        w.close()
        w = recordio.writer("/tmp/record_1")
        w.write("3")
        w.write("4")
        w.write("")
        w.close()

        r = recordio.reader("/tmp/record_*")
        self.assertEqual(pickle.loads(r.read()), "1")
        self.assertEqual(r.read(), "2")
        self.assertEqual(r.read(), "")
        self.assertEqual(r.read(), "3")
        self.assertEqual(r.read(), "4")
        self.assertEqual(r.read(), "")
        self.assertEqual(r.read(), None)
        self.assertEqual(r.read(), None)
        r.close()

    def test_binary_image(self):
        #write
        w = recordio.writer("/tmp/image_binary")
        with open("./images/10045_right_512", "rb") as f:
            con = f.read()

        d1 = {'img': con,
             'md5': md5.new(con).hexdigest()
        }

        #pickle
        p1 = pickle.dumps(d1, pickle.HIGHEST_PROTOCOL)
        print "in python before write:", md5.new(p1).hexdigest(), len(p1) 

        w.write(p1)
        w.close()

        #read
        r = recordio.reader("/tmp/image_binary")
        while True:
            p2 = r.read() 
            if not p2:
                break
            print "in python after  read:", md5.new(p2).hexdigest(), len(p2)

            d2 = pickle.loads(p2)
            self.assertEqual(md5.new(d2['img']).hexdigest(), d2['md5'])

        r.close()


if __name__ == '__main__':
    unittest.main()
