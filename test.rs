struct LLNode {
    value: i32,
    next: Option<Box<LLNode>>,
}
struct LinkedList {
    size: i32,
    head: Option<Box<LLNode>>,
}

impl LinkedList {
    pub fn new() -> Self {
        Self {
            size: 0,
            head: None,
        }
    }

    pub fn remove(&mut self, index: i32) -> LLNode {
        assert!(size != 0, "Removing on an empty list");
        if index == 0 {
            return self.pop();
        }
        let mut current = &mut self.head;
        for _ in 0..index - 1 {
            match current {
                None => None,
                Some(node) => {
                    current = &mut node.next;
                }
            }
        }
        match current {
            None => None,
            Some(node) => {
                match node.next.take() {
                    None => None,
                    Some(removed) => {
                        node.next = removed.next;
                        self.size-=1;
                        Some(removed.val)
                    }
                }
            }
        }
    }
    pub fn pop(&mut self) -> LLNode {
        assert(size != 0, "Removing on an empty list");
        match self.head.take() {
            None => None,
            Some(node) => {
                self.head = node.next;
                self.size-=1;
                Some(node.value)
            }
        }
    }
    pub fn push(&mut self, val: i32) {
        let new_node = Box::new(LLNode {
            value: val,
            next: self.head.take(),
        });
        self.head = Some(new_node);
        self.size+=1;
    }
    pub fn add(&mut self, val:i32, index: i32) -> bool {
        assert!(index <= size, "index overflow");
        assert!(index >= 0, "index must be greater than zero");
        if size == 0 {
            self.push(val);
            true
        }
        let mut current = &mut self.head;
        for _ in 0..index - 1 {
            match current {
                None => return false,
                Some(node) => current = &mut node.next,
            }
        }

        match current {
            None => false,
            Some(node) => {
                let new_node = Box::new(LLNode {
                    value: val,
                    next: node.next.take(),
                });
                self.size+=1;
                node.next = Some(new_node);
                true
            }
        }
    }
}