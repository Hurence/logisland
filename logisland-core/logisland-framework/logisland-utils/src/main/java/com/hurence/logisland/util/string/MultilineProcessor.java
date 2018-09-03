/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.util.string;


import com.sun.tools.javac.model.JavacElements;
import com.sun.tools.javac.processing.JavacProcessingEnvironment;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.TreeMaker;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import java.util.Set;

/**
 * credits to http://www.adrianwalker.org
 */
@SupportedAnnotationTypes({"com.hurence.logisland.util.string.Multiline"})
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public final class MultilineProcessor extends AbstractProcessor {

    private JavacElements elementUtils;
    private TreeMaker maker;

    @Override
    public void init(final ProcessingEnvironment procEnv) {
        super.init(procEnv);
        JavacProcessingEnvironment javacProcessingEnv = (JavacProcessingEnvironment) procEnv;
        this.elementUtils = javacProcessingEnv.getElementUtils();
        this.maker = TreeMaker.instance(javacProcessingEnv.getContext());
    }

    @Override
    public boolean process(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv) {

        Set<? extends Element> fields = roundEnv.getElementsAnnotatedWith(Multiline.class);
        for (Element field : fields) {
            String docComment = elementUtils.getDocComment(field);
            if (null != docComment) {
                JCTree.JCVariableDecl fieldNode = (JCTree.JCVariableDecl) elementUtils.getTree(field);
                fieldNode.init = maker.Literal(docComment);
            }
        }

        return true;
    }
}